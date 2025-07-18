from fastapi import APIRouter, Depends, HTTPException, Body, status, BackgroundTasks
from pydantic import BaseModel, Field, EmailStr, conint, validator
from typing import List, Optional, Literal
import logging
import time
import uuid
from datetime import datetime

from app.core.db import supabase
from app.core.auth import verify_jwt_token
from ._schemas import ErrorResponse

router = APIRouter(prefix="/api/v2/leads", tags=["leads"])

# Configuration
MAX_CHUNK_SIZE = 5000
BATCH_SIZE = 500

# Logger
logger = logging.getLogger(__name__)

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

def check_duplicates(realids: List[str]) -> List[str]:
    """Check for existing realids in database with chunking"""
    if not realids:
        return []
    
    chunk_size = 1000
    all_duplicates = []
    
    try:
        for i in range(0, len(realids), chunk_size):
            chunk = realids[i:i + chunk_size]
            response = supabase.from_("leads").select("realid").in_("realid", chunk).execute()
            if response.data:
                all_duplicates.extend([row["realid"] for row in response.data])
        
        return all_duplicates
    except Exception as e:
        logger.error(f"Error checking duplicates: {e}")
        return []  # Return empty list instead of raising exception

def batch_insert(records: List[dict], batch_size: int = BATCH_SIZE) -> tuple[int, int]:
    """Insert records in batches"""
    total_inserted = 0
    total_failed = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            response = supabase.from_("leads").insert(batch).execute()
            if response.data:
                total_inserted += len(response.data)
            else:
                total_failed += len(batch)
        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            total_failed += len(batch)
    
    return total_inserted, total_failed

async def process_leads_background(job_id: str, leads: List[Lead], user_id: str):
    """Background task that handles job record creation AND lead processing"""
    try:
        # 1) Insert job record to Supabase
        job_record = {
            "job_id": job_id,
            "status": "processing",
            "total_records": len(leads),
            "user_id": user_id,
            "chunk_size": len(leads),
            "batch_size": BATCH_SIZE,
            "processing_started_at": datetime.utcnow().isoformat()
        }
        
        supabase.from_("job_status").insert(job_record).execute()
        logger.info(f"Job {job_id} record created and processing started")
        
        # 2) Validate chunk size in background
        if len(leads) > MAX_CHUNK_SIZE:
            supabase.from_("job_status").update({
                "status": "failed",
                "error_message": f"Chunk size {len(leads)} exceeds maximum {MAX_CHUNK_SIZE}",
                "processing_completed_at": datetime.utcnow().isoformat(),
                "processing_time": 0
            }).eq("job_id", job_id).execute()
            logger.error(f"Job {job_id} failed: chunk size validation")
            return
        
        # 3) Process leads
        start_time = time.time()
        
        # Check duplicates
        realids = [lead.realid for lead in leads]
        existing_realids = check_duplicates(realids)
        duplicate_count = len(existing_realids)
        
        if duplicate_count > 0:
            supabase.from_("job_status").update({
                "status": "completed_with_duplicates",
                "duplicate_count": duplicate_count,
                "failed_count": len(leads),
                "error_message": f"Found {duplicate_count} duplicate realid(s)",
                "processing_completed_at": datetime.utcnow().isoformat(),
                "processing_time": time.time() - start_time
            }).eq("job_id", job_id).execute()
            logger.warning(f"Job {job_id} completed with {duplicate_count} duplicates")
            return
        
        # Process records
        records = [prepare_record(lead) for lead in leads]
        inserted_count, failed_count = batch_insert(records)
        
        # Update final status
        final_status = "completed" if failed_count == 0 else "completed_with_failures"
        supabase.from_("job_status").update({
            "status": final_status,
            "processed_count": inserted_count,
            "failed_count": failed_count,
            "error_message": "Some records failed to insert" if failed_count > 0 else None,
            "processing_completed_at": datetime.utcnow().isoformat(),
            "processing_time": time.time() - start_time
        }).eq("job_id", job_id).execute()
        
        logger.info(f"Job {job_id} completed: {inserted_count} inserted, {failed_count} failed")
        
    except Exception as e:
        logger.error(f"Background processing failed for job {job_id}: {e}")
        # Try to update job status to failed, but don't crash if it fails
        try:
            supabase.from_("job_status").update({
                "status": "failed",
                "error_message": str(e),
                "processing_completed_at": datetime.utcnow().isoformat()
            }).eq("job_id", job_id).execute()
        except Exception as update_error:
            logger.error(f"Failed to update job status to failed: {update_error}")

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
    user_id = token.get("sub")  # Get user ID from JWT token
    
    # 3) Queue background task - NO WAITING, chunk size validation moved to background
    background_tasks.add_task(
        process_leads_background,
        job_id,
        body.leads,
        user_id
    )
    
    # 4) Return response immediately - NO database operations, NO processing
    return JobResponse(
        job_id=job_id,
        status="received",
        message="Leads received and queued for processing",
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
        response = supabase.from_("job_status").select("*").eq("job_id", job_id).execute()
        
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
        
        query = supabase.from_("job_status").select("*").eq("user_id", user_id)
        
        if status_filter:
            query = query.eq("status", status_filter)
        
        response = query.order("received_at", desc=True).range(offset, offset + limit - 1).execute()
        
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
        "timestamp": datetime.utcnow().isoformat()
    }