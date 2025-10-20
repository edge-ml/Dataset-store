from typing import Annotated
from fastapi import APIRouter, Depends
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from app.features.Auth.service import get_user, register_user, login_user, suggest_username_by_start
from app.features.Auth.models import RegisterModel, UserNameSuggest_Input
from app.utils.jwt import verify_token

router = APIRouter()


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

@router.post("/register", status_code=201)
async def register(user_data: RegisterModel):
    return await register_user(user_data)

@router.post("/token")
async def login(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):
    return await login_user(form_data)

@router.get("/user/me")
async def get_current_user(auth_user: str = Depends(verify_token)):
    return await get_user(auth_user)

@router.post("/user/suggest")
async def suggest_username(username_data: UserNameSuggest_Input, auth_user: str = Depends(verify_token)):
    return await suggest_username_by_start(username_data)