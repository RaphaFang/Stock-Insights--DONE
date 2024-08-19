import os
from jose import JWTError, jwt, ExpiredSignatureError
from datetime import datetime, timedelta
from fastapi.responses import JSONResponse


ALGORITHM = "RS256"
private_key_path = "/app/secrets/private_key.pem"
with open(private_key_path, 'r') as file:
    PRIVATE_KEY = file.read()
public_key_path = "/app/secrets/public_key.pem"
with open(public_key_path, 'r') as file:
    PUBLIC_KEY = file.read()

def token_creator(data: dict):
    to_encode = data.copy()
    to_encode.update({"exp": datetime.utcnow() + timedelta(days=1)})
    encoded_jwt = jwt.encode(to_encode, PRIVATE_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def token_verifier(token: str):
    if not token:
        return JSONResponse(
            status_code=403,
            content={"error": True, "message": "Please log-in to access the page."},
            headers={"WWW-Authenticate": "Bearer"},
        )
    try:
        decoded_jwt = jwt.decode(token, PUBLIC_KEY, algorithms=[ALGORITHM])
        return decoded_jwt
    
    except ExpiredSignatureError:
        return JSONResponse(
            status_code=405,
            content={"error": True, "message": "Token has expired, please login again."},
            headers={"WWW-Authenticate": "Bearer"},
        )
    except JWTError:
        return JSONResponse(
            status_code=401,
            content={"error": True, "message": "The JWT token could not be validated."},
            headers={"WWW-Authenticate": "Bearer"},
        )