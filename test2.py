import os

import jwt, time
from dotenv import load_dotenv
load_dotenv()
token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvbmVjYXJkIiwic3ViIjoiZGVtby11c2VyIiwiZXhwIjoxNzUwNDA1MDkzfQ.2ErM7eTEi3leirm2bcESoS8mwe3mbbc-UZaRvD6Y-MU"
try:
    payload = jwt.decode(token, os.getenv("JWT_SECRET"), algorithms=["HS256"])
    print("Valid until:", time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(payload["exp"])))
except jwt.ExpiredSignatureError:
    print("Token expired")
