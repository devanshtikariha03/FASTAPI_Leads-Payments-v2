from fastapi import APIRouter, Depends, HTTPException, Body, status
from pydantic import BaseModel, Field, EmailStr, conint, validator
from typing import List, Optional, Literal
from postgrest import APIError
from contextlib import asynccontextmanager
import asyncio
import logging
import uuid
from datetime import datetime

from app.core.db import supabase
from app.core.auth import verify_jwt_token
from ._schemas import ErrorResponse

router = APIRouter(prefix="/api/v2/leads", tags=["leads"])

# Updated Constants - More realistic limits
MAX_LEADS = 100000
CHUNK_SIZE = 1000  # Smaller chunks for better error handling
MAX_RETRIES = 3
RETRY_DELAY = 1
MAX_CONCURRENT_REQUESTS = 20  # Increased for better throughput
MAX_DB_CONNECTIONS = 10  # Increased for better concurrency
DUPLICATE_CHECK_CHUNK_SIZE = 500  # Smaller chunks for parallel processing

# Semaphores
request_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
db_semaphore = asyncio.Semaphore(MAX_DB_CONNECTIONS)

# Logger
logger = logging.getLogger(__name__)

# In-memory batch status with thread-safe operations
batch_status_store = {}
status_lock = asyncio.Lock()

# Schemas (keeping your existing schemas)
class Lead(BaseModel):
    date: str = Field(..., pattern=r'^\d{4}-\d{2}-\d{2}\s*-\s*\d{4}-\d{2}-\d{2}$')
    action_id: str
    realid: str = Field(..., min_length=1)
    name: str = Field(..., min_length=1)
    phone_1: conint(ge=1_000_000_000, le=9_999_999_999)
    phone_2: Optional[conint(ge=1_000_000_000, le=9_999_999_999)] = None
    email: Optional[EmailStr] = None
    gender: Literal['male', 'female']
    preferred_language: Literal['en','hi','mr','te','ta','kn','gu','bn']
    home_state: str
    segment_band: Literal['6.B0_Segment6','5.B0_Segment5','4.B0_Segment4']
    collection_stage: str
    emi_eligible_flag: bool
    priority: conint(ge=1, le=10)
    bill_date: str
    due_date: str
    total_due: conint(ge=0)
    min_due: conint(ge=0)
    stab_due: Optional[conint(ge=0)] = None
    any_dispute_raised: Optional[str] = None
    days_past_due: Optional[conint(ge=0)] = None
    app_lastvisit_timestamp_after_bill_date: Optional[str] = None
    app_payment_visit: Optional[bool] = None
    last_connected_call_time: Optional[str] = None
    last_payment_details: Optional[str] = None
    last_connected_conversation: Optional[str] = None

    @validator('due_date')
    def due_after_bill(cls, v, values):
        if 'bill_date' in values and v <= values['bill_date']:
            raise ValueError('due_date must be after bill_date')
        return v

class LeadsRequest(BaseModel):
    leads: List[Lead] = Field(..., min_items=1)

class BatchStatus(BaseModel):
    batch_id: str
    status: Literal['processing', 'completed', 'failed']
    total_records: int
    processed_records: int
    failed_records: int
    error_message: Optional[str]
    created_at: str
    updated_at: str

class ProcessingResult(BaseModel):
    success: bool
    batch_id: str
    total_records: int
    inserted_records: int
    failed_records: int
    duplicate_records: int
    error_message: Optional[str] = None

# Helpers
def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]

def to_daterange(date_str):
    parts = [p.strip() for p in date_str.split(' - ', 1)]
    if len(parts) == 2:
        return f"[{parts[0]},{parts[1]}]"
    return date_str

@asynccontextmanager
async def db_connection():
    async with db_semaphore:
        yield supabase

async def exponential_backoff_retry(func, max_retries=3, base_delay=1, *args, **kwargs):
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.warning(f"Retry {attempt + 1}: {e}, sleeping {delay}s")
                await asyncio.sleep(delay)
            else:
                logger.error(f"Final retry failed: {e}")
                raise

async def update_batch_status(batch_id: str, **updates):
    """Thread-safe batch status update"""
    async with status_lock:
        if batch_id in batch_status_store:
            batch_status_store[batch_id].update(updates)
            batch_status_store[batch_id]['updated_at'] = datetime.utcnow().isoformat()

async def check_duplicates_chunk(realids_chunk: List[str]) -> List[str]:
    """Check duplicates for a single chunk"""
    try:
        async with db_connection() as db:
            data = db.from_("leads").select("realid").in_("realid", realids_chunk).execute().data
            return [r["realid"] for r in data] if data else []
    except Exception as e:
        logger.error(f"Error checking duplicates for chunk: {e}")
        return []

async def check_duplicates_parallel(realids: List[str]) -> List[str]:
    """Check duplicates in parallel chunks"""
    chunks = list(chunked(realids, DUPLICATE_CHECK_CHUNK_SIZE))
    
    # Process chunks in parallel
    tasks = [check_duplicates_chunk(chunk) for chunk in chunks]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    conflicts = []
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Duplicate check failed: {result}")
            continue
        conflicts.extend(result)
    
    return conflicts

async def insert_chunk_safe(records_chunk: List[dict], batch_id: str) -> dict:
    """Insert a chunk with proper error handling"""
    try:
        async with db_connection() as db:
            resp = db.from_("leads").insert(records_chunk).execute()
            if not resp.data:
                raise Exception("Insert returned no data")
            
            # Update batch status
            await update_batch_status(
                batch_id,
                processed_records=batch_status_store[batch_id]['processed_records'] + len(resp.data)
            )
            
            return {"success": True, "inserted": len(resp.data), "failed": 0}
    except Exception as e:
        logger.error(f"Failed to insert chunk: {e}")
        await update_batch_status(
            batch_id,
            failed_records=batch_status_store[batch_id]['failed_records'] + len(records_chunk)
        )
        return {"success": False, "inserted": 0, "failed": len(records_chunk), "error": str(e)}

async def insert_records_parallel(records: List[dict], batch_id: str) -> dict:
    """Insert records in parallel chunks"""
    chunks = list(chunked(records, CHUNK_SIZE))
    
    # Process chunks in parallel with limited concurrency
    semaphore = asyncio.Semaphore(5)  # Limit parallel inserts
    
    async def insert_with_semaphore(chunk):
        async with semaphore:
            return await exponential_backoff_retry(insert_chunk_safe, 3, 1, chunk, batch_id)
    
    tasks = [insert_with_semaphore(chunk) for chunk in chunks]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    total_inserted = 0
    total_failed = 0
    errors = []
    
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Insert task failed: {result}")
            errors.append(str(result))
            continue
        
        total_inserted += result.get("inserted", 0)
        total_failed += result.get("failed", 0)
        if result.get("error"):
            errors.append(result["error"])
    
    return {
        "inserted": total_inserted,
        "failed": total_failed,
        "errors": errors
    }

async def process_leads_internal(body: LeadsRequest) -> ProcessingResult:
    """Main processing function with improved error handling"""
    batch_id = str(uuid.uuid4())
    
    # Initialize batch status
    async with status_lock:
        batch_status_store[batch_id] = {
            'batch_id': batch_id,
            'status': 'processing',
            'total_records': len(body.leads),
            'processed_records': 0,
            'failed_records': 0,
            'error_message': None,
            'created_at': datetime.utcnow().isoformat(),
            'updated_at': datetime.utcnow().isoformat()
        }
    
    try:
        # Check for duplicates in parallel
        realids = [l.realid for l in body.leads]
        conflicts = await exponential_backoff_retry(check_duplicates_parallel, 3, 1, realids)
        
        if conflicts:
            await update_batch_status(batch_id, status='failed', error_message=f"Duplicate realid(s) found: {len(conflicts)} duplicates")
            return ProcessingResult(
                success=False,
                batch_id=batch_id,
                total_records=len(body.leads),
                inserted_records=0,
                failed_records=len(body.leads),
                duplicate_records=len(conflicts),
                error_message=f"Duplicate realid(s): {conflicts[:10]}"
            )
        
        # Prepare records
        records = [l.dict() for l in body.leads]
        for r in records:
            if 'date' in r:
                r['date'] = to_daterange(r['date'])
        
        # Insert records in parallel
        insert_result = await insert_records_parallel(records, batch_id)
        
        # Update final status
        success = insert_result["failed"] == 0
        await update_batch_status(
            batch_id,
            status='completed' if success else 'failed',
            error_message='; '.join(insert_result["errors"]) if insert_result["errors"] else None
        )
        
        return ProcessingResult(
            success=success,
            batch_id=batch_id,
            total_records=len(body.leads),
            inserted_records=insert_result["inserted"],
            failed_records=insert_result["failed"],
            duplicate_records=0,
            error_message='; '.join(insert_result["errors"]) if insert_result["errors"] else None
        )
        
    except Exception as e:
        await update_batch_status(batch_id, status='failed', error_message=str(e))
        logger.error(f"Processing failed for batch {batch_id}: {e}")
        return ProcessingResult(
            success=False,
            batch_id=batch_id,
            total_records=len(body.leads),
            inserted_records=0,
            failed_records=len(body.leads),
            duplicate_records=0,
            error_message=str(e)
        )

@router.post("", response_model=ProcessingResult)
async def create_leads(
    body: LeadsRequest = Body(...),
    token=Depends(verify_jwt_token)
):
    """Process leads with improved concurrency handling"""
    async with request_semaphore:
        return await process_leads_internal(body)

@router.get("/status/{batch_id}", response_model=BatchStatus)
async def get_batch_status(batch_id: str):
    """Get batch processing status"""
    async with status_lock:
        if batch_id not in batch_status_store:
            raise HTTPException(status_code=404, detail="Batch not found")
        return BatchStatus(**batch_status_store[batch_id])

@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "available_request_slots": request_semaphore._value,
        "available_db_connections": db_semaphore._value,
        "active_batches": len(batch_status_store)
    }