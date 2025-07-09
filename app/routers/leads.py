from fastapi import APIRouter, Depends, HTTPException, Body, status
from pydantic import BaseModel, Field, EmailStr, conint, validator
from typing import List, Optional, Literal
import logging
import time
from datetime import datetime

from app.core.db import supabase
from app.core.auth import verify_jwt_token
from ._schemas import ErrorResponse

router = APIRouter(prefix="/api/v2/leads", tags=["leads"])

# Simple configuration
MAX_CHUNK_SIZE = 5000
BATCH_SIZE = 500  # Size for database batch inserts

# Logger
logger = logging.getLogger(__name__)

# Schemas (keeping your existing ones)
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

class ChunkResponse(BaseModel):
    success: bool
    processed_count: int
    failed_count: int
    duplicate_count: int
    error_message: Optional[str] = None
    processing_time: float

# Helper functions
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
    """Check for existing realids in database with chunking to avoid parameter limits"""
    if not realids:
        return []
    
    # Split into chunks of 500 to avoid PostgreSQL parameter size limits
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
        raise HTTPException(status_code=500, detail=f"Error checking duplicates: {str(e)}")

def batch_insert(records: List[dict], batch_size: int = BATCH_SIZE) -> tuple[int, int]:
    """Insert records in batches to avoid memory issues"""
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

@router.post("", response_model=ChunkResponse)
async def create_leads(
    body: LeadsChunk = Body(...),
    token=Depends(verify_jwt_token)
):
    """
    Process a chunk of leads (up to 5000 records)
    Since client handles chunking and concurrency, this endpoint is simplified
    """
    start_time = time.time()
    
    try:
        # Validate chunk size
        if len(body.leads) > MAX_CHUNK_SIZE:
            raise HTTPException(
                status_code=400, 
                detail=f"Chunk size {len(body.leads)} exceeds maximum {MAX_CHUNK_SIZE}"
            )
        
        # Extract realids for duplicate checking
        realids = [lead.realid for lead in body.leads]
        
        # Check for duplicates
        existing_realids = check_duplicates(realids)
        duplicate_count = len(existing_realids)
        
        if duplicate_count > 0:
            logger.warning(f"Found {duplicate_count} duplicate realids in chunk")
            # You can choose to either reject the entire chunk or filter duplicates
            # For now, rejecting the entire chunk if duplicates exist
            return ChunkResponse(
                success=False,
                processed_count=0,
                failed_count=len(body.leads),
                duplicate_count=duplicate_count,
                error_message=f"Found {duplicate_count} duplicate realid(s)",
                processing_time=time.time() - start_time
            )
        
        # Prepare records for insertion
        records = [prepare_record(lead) for lead in body.leads]
        
        # Insert records in batches
        inserted_count, failed_count = batch_insert(records)
        
        processing_time = time.time() - start_time
        
        if failed_count > 0:
            logger.warning(f"Chunk processing completed with {failed_count} failures")
        
        return ChunkResponse(
            success=failed_count == 0,
            processed_count=inserted_count,
            failed_count=failed_count,
            duplicate_count=0,
            error_message=f"Some records failed to insert" if failed_count > 0 else None,
            processing_time=processing_time
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing chunk: {e}")
        return ChunkResponse(
            success=False,
            processed_count=0,
            failed_count=len(body.leads),
            duplicate_count=0,
            error_message=str(e),
            processing_time=time.time() - start_time
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