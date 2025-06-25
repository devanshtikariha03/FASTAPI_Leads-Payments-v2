# app/routers/_schemas.py
from pydantic import BaseModel

class ErrorResponse(BaseModel):
    error: str
