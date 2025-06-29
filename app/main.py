# # app/main.py

# import os
# import time
# from datetime import datetime
# from fastapi import FastAPI, Depends, HTTPException
# from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
# from pydantic import BaseModel, Field
# from dotenv import load_dotenv
# from supabase import create_client
# from postgrest import APIError
# import jwt

# # 1. Load environment variables
# load_dotenv()
# SUPABASE_URL = os.getenv("SUPABASE_URL")
# SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
# JWT_SECRET   = os.getenv("JWT_SECRET")

# if not SUPABASE_URL or not SUPABASE_KEY or not JWT_SECRET:
#     missing = [n for n in ("SUPABASE_URL","SUPABASE_SERVICE_ROLE_KEY","JWT_SECRET") if not os.getenv(n)]
#     raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

# # 2. Initialize Supabase client
# supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# # 3. Create FastAPI app
# app = FastAPI(
#     title="OneCard & Chatbot Integration API",
#     version="1.0.0",
#     description="Receives leads, payments, and inserts Chatbot_testing2 records"
# )

# # 4. JWT auth dependency
# security = HTTPBearer()

# def verify_jwt_token(creds: HTTPAuthorizationCredentials = Depends(security)):
#     token = creds.credentials
#     try:
#         payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
#     except jwt.PyJWTError:
#         raise HTTPException(status_code=401, detail="Invalid or expired token")
#     if payload.get("iss") != "onecard" or payload.get("exp", 0) < time.time():
#         raise HTTPException(status_code=401, detail="Invalid token payload")
#     return payload

# # 5. Pydantic models
# class Lead(BaseModel):
#     realid: str
#     name:   str
#     phone:  int

# class LeadsRequest(BaseModel):
#     leads: list[Lead]

# class Payment(BaseModel):
#     realid: str
#     amount: int
#     tag:    str

# class PaymentsRequest(BaseModel):
#     payments: list[Payment]

# class ChatbotRecord(BaseModel):
#     realid: str
#     Name: str
#     Name_Hindi: str = Field(..., alias="Name - Hindi")
#     Phone: int
#     Status: str
#     Due_Amount_Hindi: str = Field(..., alias="Due Amount - Hindi")
#     Minimum_Due_Hindi: str = Field(..., alias="Minimum Due-Hindi")
#     Current_Date: str = Field(..., alias="Current Date")
#     Due_Date: str = Field(..., alias="Due Date")
#     Pending_Days: int = Field(..., alias="Pending Days")
#     Risk: str
#     EMI_Available: bool = Field(..., alias="EMI Available")
#     PrevCallContext_Available: bool = Field(..., alias="PrevCallContext_Available")
#     PrevCall_Context: str = Field(..., alias="PrevCall_Context")
#     CallBlock_ForCall: str
#     Call_ID_First_Attempt: str      = Field(..., alias="Call ID - First Attempt")
#     Call_End_Reason_First_Attempt: str = Field(..., alias="Call End Reason - First Attempt")
#     Call_Attempt_Time_First_Attempt: str = Field(..., alias="Call Attempt Time - First Attempt")
#     Date: str
#     Call_Success: bool      = Field(..., alias="Call Success")
#     Call_Duration: str      = Field(..., alias="Call Duration")
#     Call_Summary: str       = Field(..., alias="Call Summary")
#     PTP_Captured: str       = Field(..., alias="PTP Captured")
#     PTP_Amount: str         = Field(..., alias="PTP Amount")
#     PTP_Date: str           = Field(..., alias="PTP Date")
#     Refusal_to_Pay: str     = Field(..., alias="Refusal to Pay")
#     EMI_Interested: str     = Field(..., alias="EMI Interested")
#     Dispute_Description: str = Field(..., alias="Dispute Description")
#     Customer_Sentiment: str = Field(..., alias="Customer Sentiment")
#     Escalation_Required: str= Field(..., alias="Escalation Required")
#     Next_Best_Action: str   = Field(..., alias="Next Best Action")
#     Agent_Malfunctioning: str=Field(..., alias="Agent Malfunctioning")
#     Transcript: str
#     Recording: str
#     Call_Cost: str          = Field(..., alias="Call Cost")
#     Attempt: str
#     lpc_date: str
#     dnd: str
#     Link: str
#     User: str
#     AI: str
#     Complete_Chat: str      = Field(..., alias="Complete Chat")

# # 6. Whitelist of actual table columns
# ALLOWED_COLUMNS = {
#     "realid","Name","Name - Hindi","Phone","Status",
#     "Due Amount - Hindi","Minimum Due-Hindi","Current Date",
#     "Due Date","Pending Days","Risk","EMI Available",
#     "PrevCallContext_Available","PrevCall_Context","CallBlock_ForCall",
#     "Call ID - First Attempt","Call End Reason - First Attempt",
#     "Call Attempt Time - First Attempt","Date","Call Success",
#     "Call Duration","Call Summary","PTP Captured","PTP Amount",
#     "PTP Date","Refusal to Pay","EMI Interested","Dispute Description",
#     "Customer Sentiment","Escalation Required","Next Best Action",
#     "Agent Malfunctioning","Transcript","Recording","Call Cost",
#     "Attempt","lpc_date","dnd","Link","User","AI","Complete Chat"
# }

# # 7. Endpoints

# @app.get("/health")
# async def health_check():
#     return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


# @app.post("/api/v1/leads")
# async def receive_leads(body: LeadsRequest, token=Depends(verify_jwt_token)):
#     records = [l.dict() for l in body.leads]
#     try:
#         resp = supabase.from_("leads").insert(records).execute()
#     except APIError as e:
#         raise HTTPException(status_code=500, detail=e.message)
#     return {"success": True, "inserted": resp.data}


# @app.post("/api/v1/payments")
# async def receive_payments(body: PaymentsRequest, token=Depends(verify_jwt_token)):
#     records = [p.dict() for p in body.payments]
#     try:
#         resp = supabase.from_("payments").insert(records).execute()
#     except APIError as e:
#         raise HTTPException(status_code=500, detail=e.message)
#     return {"success": True, "inserted": resp.data}


# @app.post("/insert-chatbot")
# async def insert_chatbot(record: ChatbotRecord, token=Depends(verify_jwt_token)):
#     full_payload = record.model_dump(by_alias=True)
#     payload = {k: v for k, v in full_payload.items() if k in ALLOWED_COLUMNS}
#     try:
#         resp = supabase.from_("Chatbot_testing2") \
#             .insert([payload], returning="representation") \
#             .execute()
#     except APIError as e:
#         raise HTTPException(status_code=500, detail=e.message)
#     return {"inserted": resp.data}


# # 8. Run with:
# #    python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000


# #    python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# # Entry point for 'python -m uvicorn app.main:app'
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)



# # app/main.py(main)

# from fastapi import FastAPI
# from app.routers import leads, payments

# app = FastAPI(
#     title="FASTAPI–Supabase Integration",
#     version="1.0.0",
#     description="Secure endpoints for Leads and Payments"
# )

# app.include_router(leads.router)
# app.include_router(payments.router)

# @app.get("/health", summary="Health check")
# def health_check():
#     return {"status": "healthy"}


from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from app.routers import leads, payments

app = FastAPI(
    title="PredixionAI-Onecard",
    description="Secure endpoints for Integrating Predixion AI Platform with OneCard Platform"
)

# Global handler for invalid JSON / validation errors
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc: RequestValidationError):
    # Build a simple list of {loc, msg} dicts
    errors = [
        {"loc": e["loc"], "msg": e["msg"]}
        for e in exc.errors()
    ]
    return JSONResponse(
        status_code=400,
        content={"error": "Invalid request", "details": errors}
    )

app.include_router(leads.router)
app.include_router(payments.router)

@app.get("/health")
def health_check():
    return {"status": "healthy"}



