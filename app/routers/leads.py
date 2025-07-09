# leads.py
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

# Semaphores
request_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
db_semaphore = asyncio.Semaphore(MAX_DB_CONNECTIONS)

logger = logging.getLogger(__name__)

# Redis client
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost")
redis = aioredis.from_url(REDIS_URL, decode_responses=True)

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
    key = f"batch:{batch_id}"
    updates['updated_at'] = datetime.utcnow().isoformat()
    await redis.hset(key, mapping=updates)

async def get_batch_status_data(batch_id: str):
    key = f"batch:{batch_id}"
    data = await redis.hgetall(key)
    if not data:
        raise HTTPException(status_code=404, detail="Batch not found")
    data['total_records'] = int(data['total_records'])
    data['processed_records'] = int(data['processed_records'])
    data['failed_records'] = int(data['failed_records'])
    return data

async def insert_chunk_safe(records_chunk, batch_id):
    try:
        async with db_connection() as db:
            resp = db.from_("leads").insert(records_chunk).execute()
            inserted = len(resp.data or [])
            await update_batch_status(batch_id, processed_records=int(await redis.hget(f"batch:{batch_id}", "processed_records")) + inserted)
            return {"success": True, "inserted": inserted, "failed": 0}
    except Exception as e:
        await update_batch_status(batch_id, failed_records=int(await redis.hget(f"batch:{batch_id}", "failed_records")) + len(records_chunk))
        return {"success": False, "inserted": 0, "failed": len(records_chunk), "error": str(e)}

async def insert_records_parallel(records, batch_id):
    chunks = list(chunked(records, CHUNK_SIZE))
    semaphore = asyncio.Semaphore(5)
    tasks = [insert_chunk_with_semaphore(chunk, batch_id, semaphore) for chunk in chunks]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    inserted, failed, errors = 0, 0, []
    for result in results:
        if isinstance(result, dict):
            inserted += result["inserted"]
            failed += result["failed"]
            if result.get("error"):
                errors.append(result["error"])
    return {"inserted": inserted, "failed": failed, "errors": errors}

async def insert_chunk_with_semaphore(chunk, batch_id, semaphore):
    async with semaphore:
        return await exponential_backoff_retry(insert_chunk_safe, 3, 1, chunk, batch_id)

async def check_duplicates_parallel(realids):
    chunks = list(chunked(realids, DUPLICATE_CHECK_CHUNK_SIZE))
    tasks = [check_duplicates_chunk(chunk) for chunk in chunks]
    results = await asyncio.gather(*tasks)
    return [item for sublist in results for item in sublist]

async def check_duplicates_chunk(chunk):
    async with db_connection() as db:
        data = db.from_("leads").select("realid").in_("realid", chunk).execute().data
        return [r["realid"] for r in data] if data else []

async def process_leads_internal(body: LeadsRequest):
    batch_id = str(uuid.uuid4())
    await redis.hset(f"batch:{batch_id}", mapping={
        "batch_id": batch_id,
        "status": "processing",
        "total_records": len(body.leads),
        "processed_records": 0,
        "failed_records": 0,
        "error_message": "",
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat()
    })
    try:
        realids = [l.realid for l in body.leads]
        conflicts = await exponential_backoff_retry(check_duplicates_parallel, 3, 1, realids)
        if conflicts:
            await update_batch_status(batch_id, status="failed", error_message=f"{len(conflicts)} duplicates")
            return ProcessingResult(False, batch_id, len(body.leads), 0, len(body.leads), len(conflicts), f"Duplicates: {conflicts[:10]}")
        records = [l.dict() for l in body.leads]
        for r in records:
            r["date"] = to_daterange(r["date"])
        result = await insert_records_parallel(records, batch_id)
        await update_batch_status(batch_id, status="completed" if result["failed"] == 0 else "failed", error_message="; ".join(result["errors"]))
        return ProcessingResult(result["failed"] == 0, batch_id, len(body.leads), result["inserted"], result["failed"], 0, "; ".join(result["errors"]))
    except Exception as e:
        await update_batch_status(batch_id, status="failed", error_message=str(e))
        return ProcessingResult(False, batch_id, len(body.leads), 0, len(body.leads), 0, str(e))

@router.post("", response_model=ProcessingResult)
async def create_leads(body: LeadsRequest = Body(...), token=Depends(verify_jwt_token)):
    async with request_semaphore:
        return await process_leads_internal(body)

@router.get("/status/{batch_id}", response_model=BatchStatus)
async def get_batch_status(batch_id: str):
    data = await get_batch_status_data(batch_id)
    return BatchStatus(**data)

@router.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "request_slots": request_semaphore._value,
        "db_slots": db_semaphore._value
    }
