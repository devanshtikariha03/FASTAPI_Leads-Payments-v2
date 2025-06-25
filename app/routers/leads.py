# # # app/routers/leads.py

# from fastapi import APIRouter, Depends, HTTPException, status
# from pydantic import BaseModel, Field, EmailStr, conint, validator
# from typing import List, Optional, Literal
# from postgrest import APIError
# from app.core.db import supabase
# from app.core.auth import verify_jwt_token

# router = APIRouter(prefix="/api/v1/leads", tags=["leads"])

# class Lead(BaseModel):
#     date: str = Field(
#         ...,
#         pattern=r'^\d{4}-\d{2}-\d{2}\s*-\s*\d{4}-\d{2}-\d{2}$',
#         description="DATERANGE in format YYYY-MM-DD - YYYY-MM-DD"
#     )
#     realid: str = Field(..., min_length=1)
#     name:   str = Field(..., min_length=1)
#     phone_1: conint(ge=1000000000, le=9999999999)
#     phone_2: Optional[conint(ge=1000000000, le=9999999999)] = None
#     email: Optional[EmailStr] = None                  # <--- EmailStr enforces valid email
#     gender: Literal['male', 'female']
#     preferred_language: Literal['en','hi','mr','te','ta','kn','gu','bn']
#     home_state: str
#     segment_band: Literal['6.B0_Segment6','5.B0_Segment5','4.B0_Segment4']
#     collection_stage: str
#     emi_eligible_flag: bool
#     priority: conint(ge=1, le=10)
#     bill_date: str
#     due_date: str
#     total_due: conint(ge=0)
#     min_due: conint(ge=0)
#     any_dispute_raised: Optional[str] = None
#     days_past_due: Optional[conint(ge=0)] = None
#     app_lastvisit_timestamp_after_bill_date: Optional[str] = None
#     app_payment_visit: Optional[bool] = None
#     last_connected_call_time: Optional[str] = None
#     last_payment_details: Optional[str] = None
#     last_connected_conversation: Optional[str] = None

#     @validator('due_date')
#     def due_after_bill(cls, v, values):
#         if 'bill_date' in values and v <= values['bill_date']:
#             raise ValueError('due_date must be after bill_date')
#         return v

# class LeadsRequest(BaseModel):
#     leads: List[Lead] = Field(..., min_items=1)

# @router.post("", summary="Receive and insert leads")
# def create_leads(body: LeadsRequest, token=Depends(verify_jwt_token)):
#     records = [lead.dict() for lead in body.leads]
#     try:
#         resp = supabase.from_("leads").insert(records).execute()
#     except APIError as e:
#         # Database-level error
#         raise HTTPException(status_code=500, detail=e.message)
#     return {"success": True, "inserted": len(resp.data)}



# app/routers/leads.py

from fastapi import APIRouter, Depends, HTTPException, Body, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, EmailStr, conint, validator
from typing import List, Optional, Literal
from postgrest import APIError

from app.core.db import supabase
from app.core.auth import verify_jwt_token
from ._schemas import ErrorResponse

router = APIRouter(prefix="/api/v2/leads", tags=["leads"])

MAX_LEADS = 100

class Lead(BaseModel):
    date: str = Field(
        ...,
        pattern=r'^\d{4}-\d{2}-\d{2}\s*-\s*\d{4}-\d{2}-\d{2}$',
        description="DATERANGE YYYY-MM-DD - YYYY-MM-DD"
    )
    realid: str = Field(..., min_length=1)
    name:   str = Field(..., min_length=1)
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
                    "example": {"error": "Leads API accepts 1 to 100 records per request"}
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
            detail="Leads API accepts 1 to 100 records per request"
        )

    # duplicate‐realid check
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

    # bulk insert
    records = [l.dict() for l in body.leads]
    try:
        resp = supabase.from_("leads").insert(records).execute()
    except APIError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

    return {"success": True, "inserted": len(resp.data)}
