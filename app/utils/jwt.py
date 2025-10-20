from fastapi.security import OAuth2PasswordBearer
from app.features.Auth.models import AuthModel_DB, Token
from fastapi import Request, Cookie, Depends, HTTPException, status
from typing import Callable, List, Optional, Union

import jwt
import datetime
from typing import Any, Dict
from fastapi import Depends, HTTPException, status
from passlib.context import CryptContext
import json
from app.internal.config import SECRET_KEY, ACCESS_TOKEN_EXPIRE_MINUTES, REFRESH_TOKEN_EXPIRE_DAYS

ALGORITHM = "HS256"

# Create the Password Hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


async def create_access_token(auth_user: AuthModel_DB, expires_delta: datetime.timedelta = None) -> str:
    """
    Create an access token with a short expiration time (e.g., 15 minutes).
    """
    # Find the user's role
    auth_db = await AuthModel_DB.get(auth_user.id)
    if expires_delta is None:
        expires_delta = datetime.timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    expire = datetime.datetime.now(datetime.timezone.utc) + expires_delta
    to_encode = {
        "id": str(auth_db.id),
        "exp": expire
    }
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def create_refresh_token(user: AuthModel_DB, expires_delta: datetime.timedelta = None) -> str:
    """
    Create a refresh token with a longer expiration time (e.g., 7 days).
    """
    if expires_delta is None:
        expires_delta = datetime.timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    expire = datetime.datetime.now(datetime.timezone.utc) + expires_delta
    print("Token expires at:", expire)  # Debugging line
    to_encode = {
        "id": str(user.id),
        "exp": expire
    }
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def create_token(user: AuthModel_DB) -> Token:
    access_token = await create_access_token(user)
    refresh_token = await create_refresh_token(user)
    return Token(access_token=access_token, refresh_token=refresh_token)


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def get_token_from_header_or_cookie(request: Request, type="access") -> Optional[str]:
    # 1. Try to get token from Authorization header
    auth: str = request.headers.get("Authorization")
    if auth and auth.lower().startswith("bearer "):
        return auth[7:]  # Remove "Bearer "

    # 2. Fallback to cookie
    token: str = request.cookies.get("access_token" if type == "access" else "refresh_token")
    if token:
        return token

    return None

async def _verify_token(request: Request):
    try:
        token = get_token_from_header_or_cookie(request)

        if not token:
            raise Exception("No token provided")

        # Decode the JWT token
        decoded_jwt = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = decoded_jwt.get("id")

        # Return the user if the token is valid
        if user_id is None:
            raise Exception()
        
        # Find the user and attach roles from the token
        user = await AuthModel_DB.get(user_id)
        if user is None:
            print("User not found for ID:", user_id)  # Debugging line
            raise Exception("User not found")
        
        return user
    except Exception as e:
        # Raise HTTP 401 Unauthorized if token is invalid or expired
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

async def verify_token(request: Request) -> AuthModel_DB:
    user = await _verify_token(request)
    return user
