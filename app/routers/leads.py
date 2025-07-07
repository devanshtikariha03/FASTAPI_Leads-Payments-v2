from fastapi import APIRouter, Depends, HTTPException, Body, status, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, EmailStr, conint, validator
from typing import List, Optional, Literal
from postgrest import APIError
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

async def process_batch_async(batch_id: str, records: List[dict]):
    """Background task to process large batches"""
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
                    logger.info(f"Processing chunk {i+1}, attempt {retry_count+1}")
                    resp = supabase.from_("leads").insert(batch).execute()
                    if not resp.data:
                        raise APIError("No data returned from insert")
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
                        batch_status_store[batch_id]['error_message'] = str(e)
            
            batch_status_store[batch_id]['processed_records'] = inserted
            batch_status_store[batch_id]['failed_records'] = failed
            batch_status_store[batch_id]['updated_at'] = datetime.utcnow().isoformat()
        
        batch_status_store[batch_id]['status'] = 'completed' if failed == 0 else 'failed'
        batch_status_store[batch_id]['updated_at'] = datetime.utcnow().isoformat()
        
    except Exception as e:
        logger.error(f"Batch {batch_id} failed with error: {str(e)}")
        batch_status_store[batch_id]['status'] = 'failed'
        batch_status_store[batch_id]['error_message'] = str(e)
        batch_status_store[batch_id]['updated_at'] = datetime.utcnow().isoformat()
        raise

@router.post(
    "",
    summary="Receive and insert leads",
    response_model=dict,
    responses={
        200: {"description": "Leads processed successfully"},
        202: {"description": "Batch accepted for processing"},
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
    logger.info(f"Received request for {count} leads")
    
    if count < 1 or count > MAX_LEADS:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail={"error": "Leads API accepts 1 to 100000 records per request"}
        )

    try:
        # Async duplicate check
        realids = [l.realid for l in body.leads]
        logger.info("Starting duplicate check...")
        
        duplicate_check_chunk_size = 5000
        all_conflicts = []
        
        for chunk_start in range(0, len(realids), duplicate_check_chunk_size):
            chunk_end = min(chunk_start + duplicate_check_chunk_size, len(realids))
            chunk_realids = realids[chunk_start:chunk_end]
            
            try:
                existing = supabase \
                    .from_("leads") \
                    .select("realid") \
                    .in_("realid", chunk_realids) \
                    .execute() \
                    .data
                
                if existing:
                    chunk_conflicts = [r["realid"] for r in existing]
                    all_conflicts.extend(chunk_conflicts)
            except APIError as e:
                logger.error(f"Duplicate check failed for chunk: {str(e)}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail={"error": "Internal server error during duplicate check"}
                )
        
        if all_conflicts:
            logger.warning(f"Found {len(all_conflicts)} duplicate realids")
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"error": f"Duplicate realid(s): {all_conflicts}"}
            )
        
        # Prepare records
        records = []
        for l in body.leads:
            rec = l.dict()
            if "date" in rec:
                rec["date"] = to_daterange(rec["date"])
            records.append(rec)
        
        # Large datasets use background processing
        if count > 500:
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
            
            background_tasks.add_task(process_batch_async, batch_id, records)
            
            return JSONResponse(
                status_code=202,
                content={
                    "success": True,
                    "batch_id": batch_id,
                    "message": "Batch accepted for processing",
                    "status_url": f"/api/v2/leads/status/{batch_id}"
                }
            )
        
        # Small datasets process synchronously
        inserted = 0
        for batch in chunked(records, CHUNK_SIZE):
            retry_count = 0
            batch_success = False
            
            while retry_count < MAX_RETRIES and not batch_success:
                try:
                    resp = supabase.from_("leads").insert(batch).execute()
                    if not resp.data:
                        raise APIError("No data returned from insert")
                    inserted += len(resp.data)
                    batch_success = True
                except APIError as e:
                    retry_count += 1
                    if retry_count < MAX_RETRIES:
                        logger.warning(f"Batch failed, retrying {retry_count}/{MAX_RETRIES}: {str(e)}")
                        await asyncio.sleep(RETRY_DELAY * retry_count)
                    else:
                        logger.error(f"Batch failed permanently: {str(e)}")
                        raise HTTPException(
                            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail={"error": "Internal server error"}
                        )
        
        return {"success": True, "inserted": inserted}
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "Internal server error"}
        )

@router.get(
    "/status/{batch_id}",
    response_model=BatchStatus,
    responses={
        404: {
            "model": ErrorResponse,
            "description": "Batch not found",
            "content": {
                "application/json": {
                    "example": {"error": "Batch not found"}
                }
            },
        }
    }
)
async def get_batch_status(batch_id: str):
    if batch_id not in batch_status_store:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "Batch not found"}
        )
    return batch_status_store[batch_id]