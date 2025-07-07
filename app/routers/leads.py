from fastapi import APIRouter, Depends, HTTPException, Body, status, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, EmailStr, conint, validator
from typing import List, Optional, Literal
from postgrest import APIError
import asyncio
import logging
from datetime import datetime
import uuid

from app.core.db import supabase
from app.core.auth import verify_jwt_token
from ._schemas import ErrorResponse

router = APIRouter(prefix="/api/v2/leads", tags=["leads"])

# Handle frontend's 100k records requirement
MAX_LEADS = 100000  # Frontend sends 100k records
CHUNK_SIZE = 200    # Smaller chunks for 100k records reliability
MAX_RETRIES = 3
RETRY_DELAY = 1    # seconds

logger = logging.getLogger(__name__)

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
    error_message: Optional[str] = None
    created_at: str
    updated_at: str

# Store for tracking batch status (in production, use Redis or DB)
batch_status_store = {}

async def process_batch_async(batch_id: str, records: List[dict]):
    """Process a batch of records asynchronously with retry logic"""
    try:
        batch_status_store[batch_id]['status'] = 'processing'
        batch_status_store[batch_id]['updated_at'] = datetime.utcnow().isoformat()
        
        inserted = 0
        failed = 0
        
        for chunk in chunked(records, CHUNK_SIZE):
            retry_count = 0
            chunk_success = False
            
            while retry_count < MAX_RETRIES and not chunk_success:
                try:
                    resp = supabase.from_("leads").insert(chunk).execute()
                    if resp.data:
                        inserted += len(resp.data)
                        chunk_success = True
                    else:
                        raise Exception("No data returned from insert")
                        
                except Exception as e:
                    retry_count += 1
                    if retry_count < MAX_RETRIES:
                        logger.warning(f"Batch {batch_id} chunk failed, retrying {retry_count}/{MAX_RETRIES}: {str(e)}")
                        await asyncio.sleep(RETRY_DELAY * retry_count)  # Exponential backoff
                    else:
                        logger.error(f"Batch {batch_id} chunk failed after {MAX_RETRIES} retries: {str(e)}")
                        failed += len(chunk)
            
            # Update progress
            batch_status_store[batch_id]['processed_records'] = inserted
            batch_status_store[batch_id]['failed_records'] = failed
            batch_status_store[batch_id]['updated_at'] = datetime.utcnow().isoformat()
        
        # Final status update
        if failed == 0:
            batch_status_store[batch_id]['status'] = 'completed'
        else:
            batch_status_store[batch_id]['status'] = 'failed'
            batch_status_store[batch_id]['error_message'] = f"{failed} records failed to insert"
            
        batch_status_store[batch_id]['updated_at'] = datetime.utcnow().isoformat()
        
    except Exception as e:
        logger.error(f"Batch {batch_id} processing failed: {str(e)}")
        batch_status_store[batch_id]['status'] = 'failed'
        batch_status_store[batch_id]['error_message'] = str(e)
        batch_status_store[batch_id]['updated_at'] = datetime.utcnow().isoformat()

@router.post(
    "",
    summary="Receive and insert leads",
    response_model=dict,
    responses={
        202: {"description": "Leads batch accepted for processing"},
        400: {
            "model": ErrorResponse,
            "description": "Validation error or bad request",
            "content": {
                "application/json": {
                    "example": {"error": "realid is required"}
                }
            },
        },
        401: {
            "model": ErrorResponse,
            "description": "Invalid or expired token",
            "content": {
                "application/json": {
                    "example": {"error": "Invalid or expired token"}
                }
            },
        },
        403: {
            "model": ErrorResponse,
            "description": "Authentication required",
            "content": {
                "application/json": {
                    "example": {"error": "Authentication required"}
                }
            },
        },
        409: {
            "model": ErrorResponse,
            "description": "Conflict — duplicate realid",
            "content": {
                "application/json": {
                    "example": {"error": "Duplicate realid(s): ['OC123']"}
                }
            },
        },
        413: {
            "model": ErrorResponse,
            "description": "Payload too large or too many records",
            "content": {
                "application/json": {
                    "example": {"error": "Leads API accepts 1 to 100000 records per request"}
                }
            },
        },
        500: {
            "model": ErrorResponse,
            "description": "Internal server error",
            "content": {
                "application/json": {
                    "example": {"error": "Internal server error"}
                }
            },
        },
    },
)
async def create_leads(
    background_tasks: BackgroundTasks,
    body: LeadsRequest = Body(...),
    token=Depends(verify_jwt_token)
):
    count = len(body.leads)
    if count < 1 or count > MAX_LEADS:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Leads API accepts 1 to {MAX_LEADS} records per request"
        )

    # Strategic duplicate check - check in batches to avoid timeout
    realids = [l.realid for l in body.leads]
    
    # For 100k records, check duplicates in chunks to avoid DB timeout
    duplicate_check_chunk_size = 5000
    all_conflicts = []
    
    for chunk_start in range(0, len(realids), duplicate_check_chunk_size):
        chunk_end = min(chunk_start + duplicate_check_chunk_size, len(realids))
        chunk_realids = realids[chunk_start:chunk_end]
        
        existing = supabase \
            .from_("leads") \
            .select("realid") \
            .in_("realid", chunk_realids) \
            .execute() \
            .data
        
        if existing:
            chunk_conflicts = [r["realid"] for r in existing]
            all_conflicts.extend(chunk_conflicts)
    
    if all_conflicts:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Duplicate realid(s): {all_conflicts[:10]}..."  # Show first 10
        )

    # Convert leads to records
    def to_daterange(date_str):
        parts = [p.strip() for p in date_str.split(' - ', 1)]
        if len(parts) == 2:
            return f"[{parts[0]},{parts[1]}]"
        return date_str

    records = []
    for l in body.leads:
        rec = l.dict()
        if "date" in rec:
            rec["date"] = to_daterange(rec["date"])
        records.append(rec)

    # Create batch tracking
    batch_id = str(uuid.uuid4())
    batch_status_store[batch_id] = {
        'batch_id': batch_id,
        'status': 'processing',
        'total_records': count,
        'processed_records': 0,
        'failed_records': 0,
        'error_message': None,
        'created_at': datetime.utcnow().isoformat(),
        'updated_at': datetime.utcnow().isoformat()
    }

    # Process asynchronously
    background_tasks.add_task(process_batch_async, batch_id, records)

    return JSONResponse(
        status_code=202,
        content={
            "success": True,
            "batch_id": batch_id,
            "total_records": count,
            "message": "Batch accepted for processing",
            "status_url": f"/api/v2/leads/status/{batch_id}"
        }
    )

@router.get(
    "/status/{batch_id}",
    summary="Get batch processing status",
    response_model=BatchStatus,
    responses={
        200: {"description": "Batch status retrieved successfully"},
        404: {"description": "Batch not found"},
    },
)
async def get_batch_status(batch_id: str):
    """Get the status of a batch processing job"""
    if batch_id not in batch_status_store:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Batch not found"
        )
    
    return BatchStatus(**batch_status_store[batch_id])

@router.post(
    "/sync",
    summary="Synchronous lead insertion (for smaller batches)",
    response_model=dict,
    responses={
        200: {"description": "Leads processed successfully"},
        413: {"description": "Batch too large for synchronous processing"},
    },
)
async def create_leads_sync(
    body: LeadsRequest = Body(...),
    token=Depends(verify_jwt_token)
):
    """Synchronous endpoint for smaller batches (max 1000 records)"""
    count = len(body.leads)
    if count > 1000:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Synchronous endpoint accepts max 1000 records. Use async endpoint for larger batches."
        )
    
    # Use original synchronous logic for small batches
    realids = [l.realid for l in body.leads]
    existing = supabase \
        .from_("leads") \
        .select("realid") \
        .in_("realid", realids) \
        .execute() \
        .data
    
    if existing:
        conflicts = [r["realid"] for r in existing]
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Duplicate realid(s): {conflicts}"
        )

    def to_daterange(date_str):
        parts = [p.strip() for p in date_str.split(' - ', 1)]
        if len(parts) == 2:
            return f"[{parts[0]},{parts[1]}]"
        return date_str

    records = []
    for l in body.leads:
        rec = l.dict()
        if "date" in rec:
            rec["date"] = to_daterange(rec["date"])
        records.append(rec)

    inserted = 0
    try:
        for batch in chunked(records, CHUNK_SIZE):
            resp = supabase.from_("leads").insert(batch).execute()
            if not resp.data:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Internal server error"
                )
            inserted += len(resp.data)
    except APIError as e:
        logger.error(f"Supabase API error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

    return {"success": True, "inserted": inserted}

def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]