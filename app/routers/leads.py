from fastapi import APIRouter, Depends, HTTPException, Body, status, BackgroundTasks
from pydantic import BaseModel, Field, EmailStr, conint, validator
from typing import List, Optional, Literal
import logging
import time
import uuid
import asyncio
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import threading
import random
from dataclasses import dataclass

from app.core.db import supabase
from app.core.auth import verify_jwt_token
from ._schemas import ErrorResponse

router = APIRouter(prefix="/api/v2/leads", tags=["leads"])

# Configuration
MAX_CHUNK_SIZE = 5000
BATCH_SIZE = 500
MAX_CONCURRENT_BACKGROUND_TASKS = 3  # Limit concurrent background tasks
MAX_DB_CONNECTIONS = 10  # Limit database connections

# Retry Configuration - Optimized for speed
MAX_RETRIES = 2  # Reduced from 3 to 2 for faster processing
RETRY_DELAY_BASE = 0.5  # Reduced from 1s to 0.5s for faster retries
RETRY_DELAY_MAX = 8  # Reduced from 16s to 8s maximum delay
RETRY_BACKOFF_MULTIPLIER = 2  # Exponential backoff multiplier

# Logger
logger = logging.getLogger(__name__)

# Concurrency control
background_semaphore = asyncio.Semaphore(MAX_CONCURRENT_BACKGROUND_TASKS)
db_semaphore = asyncio.Semaphore(MAX_DB_CONNECTIONS)

# Thread pool for CPU-intensive operations
thread_pool = ThreadPoolExecutor(max_workers=5)

# Lock for database operations
db_lock = threading.Lock()

@dataclass
class RetryStats:
    total_attempts: int = 0
    successful_retries: int = 0
    failed_after_retries: int = 0
    retry_delays: List[float] = None
    
    def __post_init__(self):
        if self.retry_delays is None:
            self.retry_delays = []

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

class LeadsChunk(BaseModel):
    leads: List[Lead] = Field(..., min_items=1, max_items=MAX_CHUNK_SIZE)

class JobResponse(BaseModel):
    job_id: str
    status: str
    message: str
    received_count: int
    timestamp: datetime

def calculate_retry_delay(attempt: int, base_delay: float = RETRY_DELAY_BASE) -> float:
    """Calculate exponential backoff delay with jitter - optimized for speed"""
    delay = min(base_delay * (RETRY_BACKOFF_MULTIPLIER ** attempt), RETRY_DELAY_MAX)
    # Reduced jitter for faster processing
    jitter = random.uniform(0.05, 0.15) * delay
    return delay + jitter

def is_retryable_error(error: Exception) -> bool:
    """Determine if an error is retryable"""
    error_msg = str(error).lower()
    retryable_patterns = [
        'timeout',
        'connection',
        'network',
        'temporary',
        'rate limit',
        'server error',
        'service unavailable',
        'too many requests',
        'deadlock',
        'lock timeout',
        'connection reset',
        'read timeout',
        'write timeout'
    ]
    
    return any(pattern in error_msg for pattern in retryable_patterns)

def to_daterange(date_str: str) -> str:
    """Convert date string to PostgreSQL daterange format"""
    parts = [p.strip() for p in date_str.split(' - ', 1)]
    if len(parts) == 2:
        return f"[{parts[0]},{parts[1]}]"
    return date_str

def prepare_record(lead: Lead) -> dict:
    """Prepare a single lead record for database insertion"""
    record = lead.dict()
    if 'date' in record:
        record['date'] = to_daterange(record['date'])
    return record

async def safe_db_operation(operation_func, *args, **kwargs):
    """Wrapper for database operations with semaphore and error handling"""
    async with db_semaphore:
        try:
            return await asyncio.get_event_loop().run_in_executor(
                thread_pool, operation_func, *args, **kwargs
            )
        except Exception as e:
            logger.error(f"Database operation failed: {e}")
            raise

def sync_check_duplicates_with_retry(realids: List[str]) -> tuple[List[str], RetryStats]:
    """Check for existing realids in database with retry mechanism"""
    if not realids:
        return [], RetryStats()
    
    retry_stats = RetryStats()
    chunk_size = 500
    all_duplicates = []
    
    for i in range(0, len(realids), chunk_size):
        chunk = realids[i:i + chunk_size]
        
        for attempt in range(MAX_RETRIES + 1):
            try:
                retry_stats.total_attempts += 1
                response = supabase.from_("leads").select("realid").in_("realid", chunk).execute()
                
                if response.data:
                    all_duplicates.extend([row["realid"] for row in response.data])
                
                if attempt > 0:
                    retry_stats.successful_retries += 1
                    logger.info(f"Duplicate check succeeded on attempt {attempt + 1}")
                
                break  # Success, break retry loop
                
            except Exception as e:
                logger.warning(f"Duplicate check attempt {attempt + 1} failed: {e}")
                
                if attempt == MAX_RETRIES:
                    # Final attempt failed
                    retry_stats.failed_after_retries += 1
                    logger.error(f"Duplicate check failed after {MAX_RETRIES + 1} attempts: {e}")
                    # Continue with next chunk instead of failing completely
                    break
                
                if is_retryable_error(e):
                    delay = calculate_retry_delay(attempt)
                    retry_stats.retry_delays.append(delay)
                    logger.info(f"Retrying duplicate check in {delay:.2f} seconds...")
                    time.sleep(delay)
                else:
                    # Non-retryable error, fail immediately
                    retry_stats.failed_after_retries += 1
                    logger.error(f"Non-retryable error in duplicate check: {e}")
                    break
    
    return all_duplicates, retry_stats

def sync_batch_insert_with_retry(records: List[dict], batch_size: int = BATCH_SIZE) -> tuple[int, int, RetryStats]:
    """Insert records in batches with retry mechanism"""
    total_inserted = 0
    total_failed = 0
    retry_stats = RetryStats()
    
    # Use even smaller batches for better performance and reliability
    actual_batch_size = min(batch_size, 50)  # Reduced from 100 to 50
    
    for i in range(0, len(records), actual_batch_size):
        batch = records[i:i + actual_batch_size]
        batch_inserted = False
        
        for attempt in range(MAX_RETRIES + 1):
            try:
                retry_stats.total_attempts += 1
                response = supabase.from_("leads").insert(batch).execute()
                
                if response.data:
                    inserted_count = len(response.data)
                    total_inserted += inserted_count
                    logger.debug(f"Batch {i//actual_batch_size + 1} inserted {inserted_count} records")
                else:
                    # No data returned but no error - treat as failure
                    total_failed += len(batch)
                    logger.warning(f"Batch {i//actual_batch_size + 1} returned no data")
                
                if attempt > 0:
                    retry_stats.successful_retries += 1
                    logger.info(f"Batch insert succeeded on attempt {attempt + 1}")
                
                batch_inserted = True
                break  # Success, break retry loop
                
            except Exception as e:
                logger.warning(f"Batch insert attempt {attempt + 1} failed: {e}")
                
                if attempt == MAX_RETRIES:
                    # Final attempt failed
                    retry_stats.failed_after_retries += 1
                    total_failed += len(batch)
                    logger.error(f"Batch insert failed after {MAX_RETRIES + 1} attempts: {e}")
                    break
                
                if is_retryable_error(e):
                    delay = calculate_retry_delay(attempt)
                    retry_stats.retry_delays.append(delay)
                    logger.info(f"Retrying batch insert in {delay:.2f} seconds...")
                    time.sleep(delay)
                else:
                    # Non-retryable error, fail immediately
                    retry_stats.failed_after_retries += 1
                    total_failed += len(batch)
                    logger.error(f"Non-retryable error in batch insert: {e}")
                    break
        
        if not batch_inserted:
            logger.error(f"Batch {i//actual_batch_size + 1} completely failed")
    
    return total_inserted, total_failed, retry_stats

def sync_update_job_status_with_retry(job_id: str, update_data: dict) -> bool:
    """Update job status with retry mechanism"""
    retry_stats = RetryStats()
    
    for attempt in range(MAX_RETRIES + 1):
        try:
            retry_stats.total_attempts += 1
            supabase.from_("job_status").update(update_data).eq("job_id", job_id).execute()
            
            if attempt > 0:
                retry_stats.successful_retries += 1
                logger.info(f"Job status update succeeded on attempt {attempt + 1}")
            
            return True  # Success
            
        except Exception as e:
            logger.warning(f"Job status update attempt {attempt + 1} failed: {e}")
            
            if attempt == MAX_RETRIES:
                logger.error(f"Job status update failed after {MAX_RETRIES + 1} attempts: {e}")
                return False
            
            if is_retryable_error(e):
                delay = calculate_retry_delay(attempt)
                retry_stats.retry_delays.append(delay)
                logger.info(f"Retrying job status update in {delay:.2f} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"Non-retryable error in job status update: {e}")
                return False
    
    return False

def sync_insert_job_record_with_retry(job_record: dict) -> bool:
    """Insert job record with retry mechanism"""
    retry_stats = RetryStats()
    
    for attempt in range(MAX_RETRIES + 1):
        try:
            retry_stats.total_attempts += 1
            supabase.from_("job_status").insert(job_record).execute()
            
            if attempt > 0:
                retry_stats.successful_retries += 1
                logger.info(f"Job record insert succeeded on attempt {attempt + 1}")
            
            return True  # Success
            
        except Exception as e:
            logger.warning(f"Job record insert attempt {attempt + 1} failed: {e}")
            
            if attempt == MAX_RETRIES:
                logger.error(f"Job record insert failed after {MAX_RETRIES + 1} attempts: {e}")
                return False
            
            if is_retryable_error(e):
                delay = calculate_retry_delay(attempt)
                retry_stats.retry_delays.append(delay)
                logger.info(f"Retrying job record insert in {delay:.2f} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"Non-retryable error in job record insert: {e}")
                return False
    
    return False

async def process_leads_background(job_id: str, leads: List[Lead], user_id: str):
    """Background task with comprehensive retry mechanisms"""
    # Wait for semaphore slot
    async with background_semaphore:
        logger.info(f"Job {job_id} acquired semaphore slot, starting processing...")
        
        try:
            # 1) Insert job record with retry
            job_record = {
                "job_id": job_id,
                "status": "processing",
                "total_records": len(leads),
                "user_id": user_id,
                "chunk_size": len(leads),
                "batch_size": BATCH_SIZE,
                "processing_started_at": datetime.utcnow().isoformat()
            }
            
            job_record_inserted = await safe_db_operation(sync_insert_job_record_with_retry, job_record)
            if not job_record_inserted:
                logger.error(f"Job {job_id} failed to insert job record after retries")
                return
            
            logger.info(f"Job {job_id} record created and processing started")
            
            # 2) Validate chunk size
            if len(leads) > MAX_CHUNK_SIZE:
                await safe_db_operation(sync_update_job_status_with_retry, job_id, {
                    "status": "failed",
                    "error_message": f"Chunk size {len(leads)} exceeds maximum {MAX_CHUNK_SIZE}",
                    "processing_completed_at": datetime.utcnow().isoformat(),
                    "processing_time": 0
                })
                logger.error(f"Job {job_id} failed: chunk size validation")
                return
            
            # 3) Process leads with timing and retry stats
            start_time = time.time()
            
            # Check duplicates with retry
            realids = [lead.realid for lead in leads]
            existing_realids, duplicate_retry_stats = await safe_db_operation(
                sync_check_duplicates_with_retry, realids
            )
            duplicate_count = len(existing_realids)
            
            if duplicate_count > 0:
                await safe_db_operation(sync_update_job_status_with_retry, job_id, {
                    "status": "completed_with_duplicates",
                    "duplicate_count": duplicate_count,
                    "failed_count": len(leads),
                    "error_message": f"Found {duplicate_count} duplicate realid(s)",
                    "processing_completed_at": datetime.utcnow().isoformat(),
                    "processing_time": time.time() - start_time,
                    "retry_stats": {
                        "duplicate_check_attempts": duplicate_retry_stats.total_attempts,
                        "duplicate_check_retries": duplicate_retry_stats.successful_retries,
                        "duplicate_check_failures": duplicate_retry_stats.failed_after_retries
                    }
                })
                logger.warning(f"Job {job_id} completed with {duplicate_count} duplicates")
                return
            
            # Process records with retry
            records = [prepare_record(lead) for lead in leads]
            inserted_count, failed_count, insert_retry_stats = await safe_db_operation(
                sync_batch_insert_with_retry, records
            )
            
            # Update final status with retry statistics
            final_status = "completed" if failed_count == 0 else "completed_with_failures"
            final_update_data = {
                "status": final_status,
                "processed_count": inserted_count,
                "failed_count": failed_count,
                "error_message": "Some records failed to insert after retries" if failed_count > 0 else None,
                "processing_completed_at": datetime.utcnow().isoformat(),
                "processing_time": time.time() - start_time,
                "retry_stats": {
                    "total_attempts": duplicate_retry_stats.total_attempts + insert_retry_stats.total_attempts,
                    "successful_retries": duplicate_retry_stats.successful_retries + insert_retry_stats.successful_retries,
                    "failed_after_retries": duplicate_retry_stats.failed_after_retries + insert_retry_stats.failed_after_retries,
                    "duplicate_check_attempts": duplicate_retry_stats.total_attempts,
                    "insert_attempts": insert_retry_stats.total_attempts,
                    "total_retry_delays": len(duplicate_retry_stats.retry_delays) + len(insert_retry_stats.retry_delays)
                }
            }
            
            await safe_db_operation(sync_update_job_status_with_retry, job_id, final_update_data)
            
            logger.info(f"Job {job_id} completed: {inserted_count} inserted, {failed_count} failed, "
                       f"{insert_retry_stats.successful_retries} successful retries, "
                       f"{insert_retry_stats.failed_after_retries} failed after retries")
            
        except Exception as e:
            logger.error(f"Background processing failed for job {job_id}: {e}")
            # Try to update job status to failed with retry
            try:
                await safe_db_operation(sync_update_job_status_with_retry, job_id, {
                    "status": "failed",
                    "error_message": str(e),
                    "processing_completed_at": datetime.utcnow().isoformat()
                })
            except Exception as update_error:
                logger.error(f"Failed to update job status to failed: {update_error}")

@router.post("/fast", response_model=JobResponse)
async def create_leads_ultra_fast(
    background_tasks: BackgroundTasks,
    body: LeadsChunk = Body(...),
    token=Depends(verify_jwt_token)
):
    """Ultra-fast endpoint - minimal validation, immediate response"""
    
    # MINIMAL validation - only check chunk size
    leads_count = len(body.leads)
    if leads_count > MAX_CHUNK_SIZE:
        raise HTTPException(status_code=400, detail="Chunk too large")
    
    # Generate job ID and queue task
    job_id = str(uuid.uuid4())
    background_tasks.add_task(process_leads_background, job_id, body.leads, token.get("sub"))
    
    # IMMEDIATE response
    return JobResponse(
        job_id=job_id,
        status="received",
        message="Queued for processing with retry mechanism",
        received_count=leads_count,
        timestamp=datetime.utcnow()
    )

@router.post("", response_model=JobResponse)
async def create_leads(
    background_tasks: BackgroundTasks,
    body: Optional[LeadsChunk] = Body(None),
    token=Depends(verify_jwt_token)
):
    """Queue leads for processing and return job ID immediately"""
    
    # 1) Validate payload exists
    if body is None or not body.leads:
        raise HTTPException(
            status_code=400, 
            detail="Request payload is required and must contain leads"
        )
    
    # 2) Create job_id and get user_id - FAST operations
    job_id = str(uuid.uuid4())
    user_id = token.get("sub")
    
    # 3) Queue background task - NO WAITING
    background_tasks.add_task(
        process_leads_background,
        job_id,
        body.leads,
        user_id
    )
    
    # 4) Return response immediately
    return JobResponse(
        job_id=job_id,
        status="received",
        message="Leads received and queued for processing with retry mechanism",
        received_count=len(body.leads),
        timestamp=datetime.utcnow() 
    )

@router.get("/status/{job_id}")
async def get_job_status(
    job_id: str,
    token=Depends(verify_jwt_token)
):
    """Get current status of a processing job"""
    try:
        # Use semaphore for status check too
        async with db_semaphore:
            response = await asyncio.get_event_loop().run_in_executor(
                thread_pool,
                lambda: supabase.from_("job_status").select("*").eq("job_id", job_id).execute()
            )
        
        if not response.data:
            raise HTTPException(status_code=404, detail="Job not found")
            
        return response.data[0]
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get job status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get job status"
        )

@router.get("/jobs")
async def get_user_jobs(
    token=Depends(verify_jwt_token),
    limit: int = 50,
    offset: int = 0,
    status_filter: Optional[str] = None
):
    """Get jobs for the current user with pagination"""
    try:
        user_id = token.get("sub")
        
        async with db_semaphore:
            def get_jobs():
                query = supabase.from_("job_status").select("*").eq("user_id", user_id)
                if status_filter:
                    query = query.eq("status", status_filter)
                return query.order("received_at", desc=True).range(offset, offset + limit - 1).execute()
            
            response = await asyncio.get_event_loop().run_in_executor(thread_pool, get_jobs)
        
        return response.data
        
    except Exception as e:
        logger.error(f"Failed to get user jobs: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get user jobs"
        )

@router.get("/health")
async def health_check():
    """Simple health check endpoint"""
    return {
        "status": "healthy",
        "max_chunk_size": MAX_CHUNK_SIZE,
        "batch_size": BATCH_SIZE,
        "max_concurrent_tasks": MAX_CONCURRENT_BACKGROUND_TASKS,
        "max_db_connections": MAX_DB_CONNECTIONS,
        "retry_config": {
            "max_retries": MAX_RETRIES,
            "base_delay": RETRY_DELAY_BASE,
            "max_delay": RETRY_DELAY_MAX,
            "backoff_multiplier": RETRY_BACKOFF_MULTIPLIER
        },
        "timestamp": datetime.utcnow().isoformat()
    }

@router.get("/metrics")
async def get_metrics():
    """Get current system metrics"""
    return {
        "background_tasks_available": background_semaphore._value,
        "db_connections_available": db_semaphore._value,
        "thread_pool_active": thread_pool._threads,
        "timestamp": datetime.utcnow().isoformat()
    }