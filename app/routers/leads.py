from fastapi import APIRouter, Depends, HTTPException, Body, status, BackgroundTasks
from fastapi.responses import JSONResponse
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

MAX_LEADS = 100000
CHUNK_SIZE = 5000
MAX_RETRIES = 3
RETRY_DELAY = 1

# Rate limiting and connection pooling
MAX_CONCURRENT_REQUESTS = 4  # Adjust based on your Azure limits
MAX_DB_CONNECTIONS = 3       # Adjust based on your database capacity
request_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
db_semaphore = asyncio.Semaphore(MAX_DB_CONNECTIONS)

logger = logging.getLogger(__name__)

# In-memory batch tracking (replace with Redis/DB in production)
batch_status_store = {}

class Lead(BaseModel):
    date: str = Field(
        ...,
        pattern=r'^\d{4}-\d{2}-\d{2}\s*-\s*\d{4}-\d{2}-\d{2}$',
        description="DATERANGE YYYY-MM-DD - YYYY-MM-DD"
    )
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
    """Database connection context manager with semaphore"""
    async with db_semaphore:
        try:
            yield supabase
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise

async def exponential_backoff_retry(func, max_retries=3, base_delay=1, *args, **kwargs):
    """Exponential backoff retry wrapper"""
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.warning(f"Operation failed, retrying in {delay}s (attempt {attempt + 1}/{max_retries}): {e}")
                await asyncio.sleep(delay)
            else:
                logger.error(f"Operation failed after {max_retries} attempts: {e}")
                raise

async def process_batch_async(batch_id: str, records: List[dict]):
    """Background task to process large batches with connection pooling"""
    try:
        batch_status_store[batch_id]['status'] = 'processing'
        inserted = 0
        failed = 0
        
        logger.info(f"Starting batch {batch_id} with {len(records)} records")
        
        for i, batch in enumerate(chunked(records, CHUNK_SIZE)):
            retry_count = 0
            batch_success = False
            
            while retry_count < MAX_RETRIES and not batch_success:
                try:
                    async with db_connection() as db:
                        logger.info(f"Processing chunk {i+1}, attempt {retry_count+1}")
                        resp = db.from_("leads").insert(batch).execute()
                        if not resp.data:
                            raise Exception("No data returned from insert")
                        inserted += len(resp.data)
                        batch_success = True
                        logger.info(f"Chunk {i+1} successful: {len(resp.data)} records")
                except Exception as e:
                    retry_count += 1
                    if retry_count < MAX_RETRIES:
                        logger.warning(f"Chunk {i+1} failed, retrying {retry_count}/{MAX_RETRIES}: {str(e)}")
                        await asyncio.sleep(RETRY_DELAY * retry_count)
                    else:
                        logger.error(f"Chunk {i+1} failed permanently: {str(e)}")
                        failed += len(batch)
            
            # Update status after each chunk
            batch_status_store[batch_id]['processed_records'] = inserted
            batch_status_store[batch_id]['failed_records'] = failed
            batch_status_store[batch_id]['updated_at'] = datetime.utcnow().isoformat()
        
        # Final status update
        batch_status_store[batch_id]['status'] = 'completed' if failed == 0 else 'failed'
        batch_status_store[batch_id]['updated_at'] = datetime.utcnow().isoformat()
        
        logger.info(f"Batch {batch_id} completed: {inserted} inserted, {failed} failed")
        
    except Exception as e:
        logger.error(f"Batch {batch_id} failed with error: {str(e)}")
        batch_status_store[batch_id]['status'] = 'failed'
        batch_status_store[batch_id]['error_message'] = str(e)
        batch_status_store[batch_id]['updated_at'] = datetime.utcnow().isoformat()

async def check_duplicates_async(realids: List[str]) -> List[str]:
    """Async duplicate check in chunks with connection pooling"""
    duplicate_check_chunk_size = 1000
    all_conflicts = []
    
    for chunk_start in range(0, len(realids), duplicate_check_chunk_size):
        chunk_end = min(chunk_start + duplicate_check_chunk_size, len(realids))
        chunk_realids = realids[chunk_start:chunk_end]
        
        logger.info(f"Checking duplicates for chunk {chunk_start}-{chunk_end}")
        
        try:
            async with db_connection() as db:
                existing = db \
                    .from_("leads") \
                    .select("realid") \
                    .in_("realid", chunk_realids) \
                    .execute() \
                    .data
                
                if existing:
                    chunk_conflicts = [r["realid"] for r in existing]
                    all_conflicts.extend(chunk_conflicts)
                    
        except Exception as e:
            logger.error(f"Duplicate check failed for chunk {chunk_start}-{chunk_end}: {str(e)}")
            raise
    
    return all_conflicts

async def process_leads_internal(body: LeadsRequest):
    """Internal processing logic with connection pooling"""
    count = len(body.leads)
    
    if count < 1 or count > MAX_LEADS:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Leads API accepts 1 to 100000 records per request"
        )

    # Async duplicate check with exponential backoff
    realids = [l.realid for l in body.leads]
    logger.info("Starting duplicate check...")
    
    all_conflicts = await exponential_backoff_retry(check_duplicates_async, 3, 1, realids)
    
    if all_conflicts:
        logger.warning(f"Found {len(all_conflicts)} duplicate realids")
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Duplicate realid(s): {all_conflicts}"
        )
    
    logger.info("Duplicate check passed")
    
    # Prepare records
    records = []
    for l in body.leads:
        rec = l.dict()
        if "date" in rec:
            rec["date"] = to_daterange(rec["date"])
        records.append(rec)
    
    # Process synchronously for all requests (no background processing)
    inserted = 0
    for batch in chunked(records, CHUNK_SIZE):
        retry_count = 0
        batch_success = False
        
        while retry_count < MAX_RETRIES and not batch_success:
            try:
                async with db_connection() as db:
                    resp = db.from_("leads").insert(batch).execute()
                    if not resp.data:
                        raise Exception("No data returned from insert")
                    inserted += len(resp.data)
                    batch_success = True
            except Exception as e:
                retry_count += 1
                if retry_count < MAX_RETRIES:
                    logger.warning(f"Batch failed, retrying {retry_count}/{MAX_RETRIES}: {str(e)}")
                    await asyncio.sleep(RETRY_DELAY * retry_count)
                else:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Internal server error"
                    )
    
    logger.info(f"Synchronous processing completed: {inserted} records")
    return {"success": True, "inserted": inserted}

@router.post("")
async def create_leads(
    background_tasks: BackgroundTasks,
    body: LeadsRequest = Body(...),
    token=Depends(verify_jwt_token)
):
    """Main endpoint with request semaphore and connection pooling"""
    async with request_semaphore:
        request_start = datetime.utcnow()
        logger.info(f"Processing request for {len(body.leads)} leads (semaphore acquired)")
        
        try:
            result = await exponential_backoff_retry(
                process_leads_internal, 
                MAX_RETRIES, 
                RETRY_DELAY, 
                body
            )
            
            request_duration = (datetime.utcnow() - request_start).total_seconds()
            logger.info(f"Request completed in {request_duration:.2f}s")
            
            return result
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal server error"
            )

@router.get("/status/{batch_id}", response_model=BatchStatus)
async def get_batch_status(batch_id: str):
    """Get status of a background batch"""
    if batch_id not in batch_status_store:
        raise HTTPException(status_code=404, detail="Batch not found")
    return batch_status_store[batch_id]

@router.get("/health")
async def health_check():
    """Health check endpoint to monitor semaphore status"""
    return {
        "status": "healthy",
        "concurrent_requests_available": request_semaphore._value,
        "db_connections_available": db_semaphore._value,
        "active_batches": len(batch_status_store)
    }