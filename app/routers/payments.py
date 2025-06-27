# # app/routers/payments.py

# from fastapi import APIRouter, Depends, HTTPException
# from pydantic import BaseModel, Field
# from typing import List
# from postgrest import APIError
# from app.core.db import supabase
# from app.core.auth import verify_jwt_token

# router = APIRouter(prefix="/api/v1/payments", tags=["payments"])

# class Payment(BaseModel):
#     date: str = Field(
#         ...,
#         pattern=r'^\d{4}-\d{2}-\d{2}\s*-\s*\d{4}-\d{2}-\d{2}$',
#         description="DATERANGE in format YYYY-MM-DD - YYYY-MM-DD"
#     )
#     realid: str
#     Amount: int
#     Payment_Tag: str = Field(..., alias="Payment Tag")

# class PaymentsRequest(BaseModel):
#     payments: List[Payment]

# @router.post("", summary="Receive and insert payments")
# def create_payments(body: PaymentsRequest, token=Depends(verify_jwt_token)):
#     # Use by_alias to send "Payment Tag" key
#     records = [p.dict(by_alias=True) for p in body.payments]
#     try:
#         resp = supabase.from_("Payments").insert(records).execute()
#     except APIError as e:
#         raise HTTPException(status_code=500, detail=e.message)
#     return {"success": True, "inserted": len(resp.data)}




# app/routers/payments.py

from fastapi import APIRouter, Depends, HTTPException, Body, status
from pydantic import BaseModel, Field, conint, validator
from typing import List, Literal
from postgrest import APIError

from app.core.db import supabase
from app.core.auth import verify_jwt_token
from ._schemas import ErrorResponse

router = APIRouter(prefix="/api/v2/payments", tags=["payments"])

MAX_PAYMENTS = 50000

class Payment(BaseModel):
    date: str = Field(
        ...,
        pattern=r'^\d{4}-\d{2}-\d{2}\s*-\s*\d{4}-\d{2}-\d{2}$',
        description="DATERANGE YYYY-MM-DD - YYYY-MM-DD"
    )
    action_id: str
    realid: str = Field(..., min_length=1)
    amount: conint(gt=0)
    payment_tag: Literal['<STAB', 'STAB', '<MAD', 'MAD', '<TAD', 'TAD']

    @validator('date')
    def validate_range(cls, v):
        parts = [p.strip() for p in v.split(' - ', 1)]
        if len(parts) != 2:
            raise ValueError("date must be two dates separated by ' - '")
        return v

class PaymentsRequest(BaseModel):
    payments: List[Payment] = Field(..., min_items=1)

@router.post(
    "",
    summary="Receive and insert payments",
    response_model=dict,
    responses={
        200: {"description": "Payments processed successfully"},
        400: {
            "model": ErrorResponse,
            "description": "Validation error or bad request",
            "content": {
                "application/json": {
                    "example": {"error": "Invalid request", "details": []}
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
            "description": "Conflict — duplicate payment",
            "content": {
                "application/json": {
                    "example": {"error": "Duplicate payment record"}
                }
            },
        },
        413: {
            "model": ErrorResponse,
            "description": "Payload too large or too many records",
            "content": {
                "application/json": {
                    "example": {"error": f"Payments API accepts 1 to {MAX_PAYMENTS} records per request"}
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
def create_payments(
    body: PaymentsRequest = Body(...),
    token=Depends(verify_jwt_token)
):
    # 1) Enforce record count
    count = len(body.payments)
    if count < 1 or count > MAX_PAYMENTS:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Payments API accepts 1 to {MAX_PAYMENTS} records per request"
        )

    # 2) Prep payload
    records = [p.dict() for p in body.payments]

    # 3) Attempt insert & surface real errors
    try:
        resp = supabase.from_("payments").insert(records).execute()
    except APIError as e:
        # Log the raw Supabase error for debugging
        print("❗️ Supabase APIError:", e.message, "| code:", getattr(e, "code", None))

        # Unique-violation
        if getattr(e, "code", "") == "23505" or "duplicate key" in (e.message or "").lower():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Duplicate payment record"
            )

        # Forward client-side DB errors as 400
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=e.message
        )

    return {"success": True, "inserted": len(resp.data)}



