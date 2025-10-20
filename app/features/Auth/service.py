from fastapi import HTTPException, status, Response
from fastapi.security import OAuth2PasswordRequestForm
from app.utils.jwt import create_token
from app.features.Auth.models import RegisterModel, Token, AuthModel_DB, AuthModel, AuthModel_Output
from app.utils.jwt import create_token
from passlib.context import CryptContext
import json

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

async def register_user(user_data: RegisterModel):
    user = await AuthModel_DB.find_one(AuthModel_DB.email == user_data.email)
    if user:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Email already registered")
    
    user = await AuthModel_DB.find_one(AuthModel_DB.username == user_data.username)
    if user:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Username already taken")

    password_hash = pwd_context.hash(user_data.password)

    new_user = AuthModel_DB(
        username=user_data.username,
        hashed_password=password_hash,
        email=user_data.email
    )
    await new_user.insert()
    return

async def login_user(form_data: OAuth2PasswordRequestForm) -> AuthModel_DB:
    user = await AuthModel_DB.find_one(AuthModel_DB.username == form_data.username)
    if not user:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password")

    if not pwd_context.verify(form_data.password, user.hashed_password):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password")

    tokens = await create_token(user)

    response = Response(content=None)
    response.set_cookie(key="access_token", value=tokens.access_token, httponly=True)
    response.set_cookie(key="refresh_token", value=tokens.refresh_token, httponly=True)
    return response


async def get_user(auth_user: AuthModel_DB):
    return AuthModel_Output(**auth_user.model_dump())

async def suggest_username_by_start(username_data):
    username = username_data.username
    # Find username that starts with the given username
    existing_usernames = await AuthModel_DB.find({"username": {"$regex": f"^{username}"}}).to_list()
    return existing_usernames