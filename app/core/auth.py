# # app/core/auth.py

# import os
# import time
# import jwt
# from dotenv import load_dotenv
# from fastapi import Depends, HTTPException, status
# from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

# # Load environment variables
# load_dotenv()
# JWT_SECRET = os.getenv("JWT_SECRET")
# if not JWT_SECRET:
#     raise RuntimeError("Missing JWT_SECRET in .env")

# # HTTPBearer instance for dependency
# security = HTTPBearer()

# def verify_jwt_token(creds: HTTPAuthorizationCredentials = Depends(security)) -> dict:
#     """
#     Verify incoming JWT Bearer token.
#     Raises 401 if invalid or expired.
#     """
#     token = creds.credentials
#     try:
#         payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
#     except jwt.PyJWTError:
#         raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token")

#     # Validate issuer and expiry
#     if payload.get("iss") != "onecard" or payload.get("exp", 0) < time.time():
#         raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload")

#     return payload


# app/core/auth.py

import os
import time
import jwt
from dotenv import load_dotenv
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

# Load environment variables
load_dotenv()
JWT_SECRET = os.getenv("JWT_SECRET")
if not JWT_SECRET:
    raise RuntimeError("Missing JWT_SECRET in .env")

class JWTBearer(HTTPBearer):
    """
    Subclass HTTPBearer so that:
    - missing header → 401 Unauthorized (instead of 403)
    - otherwise just return credentials
    """
    async def __call__(self, request: Request) -> HTTPAuthorizationCredentials:
        creds = await super().__call__(request)
        if creds is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication required"
            )
        return creds

# use our subclass here
security = JWTBearer()

def verify_jwt_token(
    creds: HTTPAuthorizationCredentials = Depends(security)
) -> dict:
    """
    Decode and verify the JWT from the Bearer header.
    Raises 401 Unauthorized for:
      - expired tokens (“Token expired”)
      - any other decode issue (“Invalid token”)
      - bad issuer or past-expiry in payload
    Returns the payload dict on success.
    """
    token = creds.credentials
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        # Signature valid but expired
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired"
        )
    except jwt.InvalidTokenError:
        # Any other decode error
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )

    # Additional payload checks
    if payload.get("iss") != "onecard":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token issuer"
        )
    if payload.get("exp", 0) < time.time():
        # just in case
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired"
        )

    return payload
