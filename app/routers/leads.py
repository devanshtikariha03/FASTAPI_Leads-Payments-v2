from fastapi import APIRouter, Depends, HTTPException, Body, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, EmailStr, conint, validator
from typing import List, Optional, Literal
from postgrest import APIError
import asyncio
import logging

from app.core.db import supabase
from app.core.auth import verify_jwt_token
from ._schemas import ErrorResponse

router = APIRouter(prefix="/api/v2/leads", tags=["leads"])

MAX_LEADS = 100000
CHUNK_SIZE = 200   # Smaller chunks for better reliability
MAX_RETRIES = 3
RETRY_DELAY = 1

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
    # only min_items enforced here—max handled manually
    leads: List[Lead] = Field(..., min_items=1)

@router.post(
    "",
    summary="Receive and insert leads",
    response_model=dict,
    responses={
        200: {"description": "Leads processed successfully"},
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
def create_leads(
    body: LeadsRequest = Body(...),
    token=Depends(verify_jwt_token)
):
    count = len(body.leads)
    if count < 1 or count > MAX_LEADS:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Leads API accepts 1 to 100000 records per request"
        )

    # duplicate‐realid check - chunked for large datasets
    realids = [l.realid for l in body.leads]
    
    # Check duplicates in chunks to avoid timeout with 100k records
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
            detail=f"Duplicate realid(s): {all_conflicts}"
        )

    # bulk insert in chunks of 200, convert date fields to daterange
    def to_daterange(date_str):
        # Expects 'YYYY-MM-DD - YYYY-MM-DD', returns '[YYYY-MM-DD,YYYY-MM-DD]'
        parts = [p.strip() for p in date_str.split(' - ', 1)]
        if len(parts) == 2:
            return f"[{parts[0]},{parts[1]}]"
        return date_str

    records = []
    for l in body.leads:
        rec = l.dict()
        # Convert 'date' field to daterange if present
        if "date" in rec:
            rec["date"] = to_daterange(rec["date"])
        # Optionally convert bill_date and due_date if your DB expects daterange (else remove these lines)
        # if "bill_date" in rec:
        #     rec["bill_date"] = to_daterange(rec["bill_date"])
        # if "due_date" in rec:
        #     rec["due_date"] = to_daterange(rec["due_date"])
        records.append(rec)

    inserted = 0
    try:
        for batch in chunked(records, CHUNK_SIZE):
            retry_count = 0
            batch_success = False
            
            while retry_count < MAX_RETRIES and not batch_success:
                try:
                    resp = supabase.from_("leads").insert(batch).execute()
                    if not resp.data:
                        raise Exception("No data returned from insert")
                    inserted += len(resp.data)
                    batch_success = True
                except Exception as e:
                    retry_count += 1
                    if retry_count < MAX_RETRIES:
                        logger.warning(f"Batch failed, retrying {retry_count}/{MAX_RETRIES}: {str(e)}")
                        # Small delay before retry
                        import time
                        time.sleep(RETRY_DELAY * retry_count)
                    else:
                        # Final retry failed, raise original error
                        raise HTTPException(
                            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Internal server error"
                        )
                        
    except APIError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

    return {"success": True, "inserted": inserted}

def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]