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

# Constants
MAX_LEADS = 100000
CHUNK_SIZE = 5000
MAX_RETRIES = 3
RETRY_DELAY = 1
MAX_CONCURRENT_REQUESTS = 4
MAX_DB_CONNECTIONS = 3

# Semaphores
request_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
db_semaphore = asyncio.Semaphore(MAX_DB_CONNECTIONS)

# Logger
logger = logging.getLogger(__name__)

# In-memory batch status
batch_status_store = {}

# Schemas
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

async def check_duplicates_async(realids: List[str]) -> List[str]:
    conflicts = []
    for i in range(0, len(realids), 1000):
        chunk = realids[i:i+1000]
        async with db_connection() as db:
            data = db.from_("leads").select("realid").in_("realid", chunk).execute().data
            if data:
                conflicts.extend([r["realid"] for r in data])
    return conflicts

async def insert_records_in_chunks(records):
    inserted = 0
    for batch in chunked(records, CHUNK_SIZE):
        async with db_connection() as db:
            resp = db.from_("leads").insert(batch).execute()
            if not resp.data:
                raise Exception("Insert returned no data")
            inserted += len(resp.data)
    return inserted

async def process_leads_internal(body: LeadsRequest):
    realids = [l.realid for l in body.leads]
    conflicts = await exponential_backoff_retry(check_duplicates_async, 3, 1, realids)
    if conflicts:
        raise HTTPException(status_code=409, detail=f"Duplicate realid(s): {conflicts[:10]}")
    
    records = [l.dict() for l in body.leads]
    for r in records:
        if 'date' in r:
            r['date'] = to_daterange(r['date'])
    
    inserted = await exponential_backoff_retry(insert_records_in_chunks, 3, 1, records)
    return {"success": True, "inserted": inserted}

@router.post("")
async def create_leads(
    body: LeadsRequest = Body(...),
    token=Depends(verify_jwt_token)
):
    async with request_semaphore:
        return await exponential_backoff_retry(process_leads_internal, 3, 1, body)

@router.get("/status/{batch_id}", response_model=BatchStatus)
async def get_batch_status(batch_id: str):
    if batch_id not in batch_status_store:
        raise HTTPException(status_code=404, detail="Batch not found")
    return batch_status_store[batch_id]

@router.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "available_request_slots": request_semaphore._value,
        "available_db_connections": db_semaphore._value
    }
