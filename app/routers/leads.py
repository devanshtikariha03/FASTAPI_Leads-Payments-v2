from fastapi import APIRouter, Depends, HTTPException, Body, status
from pydantic import BaseModel, Field, EmailStr, conint, validator
from typing import List, Optional, Literal
from postgrest import APIError
from contextlib import asynccontextmanager
import asyncio
import logging
import uuid
from datetime import datetime
import aioredis
import os
import json

from app.core.db import supabase
from app.core.auth import verify_jwt_token
from ._schemas import ErrorResponse

router = APIRouter(prefix="/api/v2/leads", tags=["leads"])

# Constants
MAX_LEADS = 100000
CHUNK_SIZE = 1000
MAX_RETRIES = 3
RETRY_DELAY = 1
MAX_CONCURRENT_REQUESTS = 20
MAX_DB_CONNECTIONS = 10
DUPLICATE_CHECK_CHUNK_SIZE = 500
OPERATION_TIMEOUT = 300  # 5 minutes timeout

# Semaphores
request_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
db_semaphore = asyncio.Semaphore(MAX_DB_CONNECTIONS)

logger = logging.getLogger(__name__)

# Redis client with connection pooling
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost")
redis = aioredis.from_url(REDIS_URL, decode_responses=True, max_connections=20)

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
    leads: List[Lead] = Field(..., min_items=1, max_items=MAX_LEADS)

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

def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]

def to_daterange(date_str):
    parts = [p.strip() for p in date_str.split(' - ', 1)]
    return f"[{parts[0]},{parts[1]}]" if len(parts) == 2 else date_str

@asynccontextmanager
async def db_connection():
    async with db_semaphore:
        yield supabase

async def exponential_backoff_retry(func, max_retries=3, base_delay=1, *args, **kwargs):
    for attempt in range(max_retries):
        try:
            return await asyncio.wait_for(func(*args, **kwargs), timeout=30)
        except asyncio.TimeoutError:
            logger.warning(f"Timeout on attempt {attempt + 1}")
            if attempt < max_retries - 1:
                await asyncio.sleep(base_delay * (2 ** attempt))
            else:
                raise HTTPException(status_code=408, detail="Operation timed out")
        except Exception as e:
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.warning(f"Retry {attempt + 1}: {e}, sleeping {delay}s")
                await asyncio.sleep(delay)
            else:
                logger.error(f"Final retry failed: {e}")
                raise

async def atomic_update_batch_status(batch_id: str, **updates):
    """Thread-safe atomic update using Redis transactions"""
    key = f"batch:{batch_id}"
    updates['updated_at'] = datetime.utcnow().isoformat()
    
    async with redis.pipeline(transaction=True) as pipe:
        while True:
            try:
                await pipe.watch(key)
                await pipe.multi()
                await pipe.hset(key, mapping=updates)
                await pipe.expire(key, 86400)  # 24 hour expiry
                await pipe.execute()
                break
            except aioredis.WatchError:
                # Retry on watch error
                continue

async def atomic_increment_counters(batch_id: str, processed: int = 0, failed: int = 0):
    """Atomically increment counters"""
    key = f"batch:{batch_id}"
    
    async with redis.pipeline(transaction=True) as pipe:
        while True:
            try:
                await pipe.watch(key)
                current = await pipe.hgetall(key)
                
                await pipe.multi()
                if processed > 0:
                    new_processed = int(current.get('processed_records', 0)) + processed
                    await pipe.hset(key, 'processed_records', new_processed)
                
                if failed > 0:
                    new_failed = int(current.get('failed_records', 0)) + failed
                    await pipe.hset(key, 'failed_records', new_failed)
                
                await pipe.hset(key, 'updated_at', datetime.utcnow().isoformat())
                await pipe.execute()
                break
            except aioredis.WatchError:
                continue

async def get_batch_status_data(batch_id: str):
    key = f"batch:{batch_id}"
    data = await redis.hgetall(key)
    if not data:
        raise HTTPException(status_code=404, detail="Batch not found")
    
    # Convert string values to appropriate types
    data['total_records'] = int(data['total_records'])
    data['processed_records'] = int(data['processed_records'])
    data['failed_records'] = int(data['failed_records'])
    return data

async def initialize_batch_status(batch_id: str, total_records: int):
    """Initialize batch status in Redis"""
    key = f"batch:{batch_id}"
    await redis.hset(key, mapping={
        "batch_id": batch_id,
        "status": "processing",
        "total_records": total_records,
        "processed_records": 0,
        "failed_records": 0,
        "error_message": "",
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat()
    })
    await redis.expire(key, 86400)  # 24 hour expiry

async def check_duplicates_chunk(chunk: List[str]) -> List[str]:
    """Check duplicates for a single chunk with error handling"""
    try:
        async with db_connection() as db:
            data = db.from_("leads").select("realid").in_("realid", chunk).execute().data
            return [r["realid"] for r in data] if data else []
    except Exception as e:
        logger.error(f"Error checking duplicates for chunk: {e}")
        # Return empty list to continue processing instead of failing
        return []

async def check_duplicates_parallel(realids: List[str]) -> List[str]:
    """Check duplicates in parallel chunks with better error handling"""
    chunks = list(chunked(realids, DUPLICATE_CHECK_CHUNK_SIZE))
    
    # Limit concurrent duplicate checks
    semaphore = asyncio.Semaphore(8)
    
    async def check_with_semaphore(chunk):
        async with semaphore:
            return await exponential_backoff_retry(check_duplicates_chunk, 3, 1, chunk)
    
    tasks = [check_with_semaphore(chunk) for chunk in chunks]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    conflicts = []
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Duplicate check failed: {result}")
            continue
        conflicts.extend(result)
    
    return conflicts

async def insert_chunk_safe(records_chunk: List[dict], batch_id: str) -> dict:
    """Insert a chunk with proper error handling and atomic updates"""
    try:
        async with db_connection() as db:
            resp = db.from_("leads").insert(records_chunk).execute()
            inserted = len(resp.data) if resp.data else 0
            
            # Atomic counter increment
            await atomic_increment_counters(batch_id, processed=inserted)
            
            return {"success": True, "inserted": inserted, "failed": 0}
            
    except Exception as e:
        logger.error(f"Failed to insert chunk: {e}")
        
        # Atomic counter increment for failures
        await atomic_increment_counters(batch_id, failed=len(records_chunk))
        
        return {"success": False, "inserted": 0, "failed": len(records_chunk), "error": str(e)}

async def insert_records_parallel(records: List[dict], batch_id: str) -> dict:
    """Insert records in parallel chunks with controlled concurrency"""
    chunks = list(chunked(records, CHUNK_SIZE))
    
    # Limit concurrent inserts to prevent overwhelming the database
    semaphore = asyncio.Semaphore(5)
    
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
        "errors": errors[:5]  # Limit error messages
    }

async def process_leads_internal(body: LeadsRequest) -> ProcessingResult:
    """Main processing function with comprehensive error handling"""
    batch_id = str(uuid.uuid4())
    
    try:
        # Initialize batch status
        await initialize_batch_status(batch_id, len(body.leads))
        
        # Check for duplicates
        realids = [l.realid for l in body.leads]
        conflicts = await check_duplicates_parallel(realids)
        
        if conflicts:
            error_msg = f"Found {len(conflicts)} duplicate realid(s)"
            await atomic_update_batch_status(
                batch_id, 
                status="failed", 
                error_message=error_msg,
                failed_records=len(body.leads)
            )
            
            return ProcessingResult(
                success=False,
                batch_id=batch_id,
                total_records=len(body.leads),
                inserted_records=0,
                failed_records=len(body.leads),
                duplicate_records=len(conflicts),
                error_message=f"Duplicates found: {conflicts[:10]}"
            )
        
        # Prepare records for insertion
        records = []
        for lead in body.leads:
            record = lead.dict()
            record["date"] = to_daterange(record["date"])
            records.append(record)
        
        # Insert records in parallel
        insert_result = await insert_records_parallel(records, batch_id)
        
        # Update final status
        success = insert_result["failed"] == 0
        status = "completed" if success else "failed"
        error_message = "; ".join(insert_result["errors"]) if insert_result["errors"] else None
        
        await atomic_update_batch_status(
            batch_id,
            status=status,
            error_message=error_message or ""
        )
        
        return ProcessingResult(
            success=success,
            batch_id=batch_id,
            total_records=len(body.leads),
            inserted_records=insert_result["inserted"],
            failed_records=insert_result["failed"],
            duplicate_records=0,
            error_message=error_message
        )
        
    except Exception as e:
        logger.error(f"Processing failed for batch {batch_id}: {e}")
        await atomic_update_batch_status(
            batch_id,
            status="failed",
            error_message=str(e),
            failed_records=len(body.leads)
        )
        
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
    """Process leads with improved concurrency and error handling"""
    async with request_semaphore:
        try:
            return await asyncio.wait_for(
                process_leads_internal(body),
                timeout=OPERATION_TIMEOUT
            )
        except asyncio.TimeoutError:
            logger.error("Lead processing timed out")
            raise HTTPException(
                status_code=408,
                detail="Processing timed out. Please try with smaller batch size."
            )

@router.get("/status/{batch_id}", response_model=BatchStatus)
async def get_batch_status(batch_id: str):
    """Get batch processing status"""
    data = await get_batch_status_data(batch_id)
    return BatchStatus(**data)

@router.get("/health")
async def health_check():
    """Enhanced health check"""
    try:
        # Test Redis connection
        await redis.ping()
        redis_status = "healthy"
    except Exception as e:
        redis_status = f"unhealthy: {e}"
    
    return {
        "status": "healthy",
        "request_slots": request_semaphore._value,
        "db_slots": db_semaphore._value,
        "redis_status": redis_status,
        "max_concurrent_requests": MAX_CONCURRENT_REQUESTS,
        "max_db_connections": MAX_DB_CONNECTIONS
    }