import os
import jwt, time
from dotenv import load_dotenv

load_dotenv()

# 30 days in seconds
ONE_MONTH = 30 * 24 * 60 * 60

payload = {
    "iss": "onecard",
    "sub": "demo-user",
    "exp": int(time.time()) + ONE_MONTH
}

token = jwt.encode(payload, os.getenv("JWT_SECRET"), algorithm="HS256")
print(token)
