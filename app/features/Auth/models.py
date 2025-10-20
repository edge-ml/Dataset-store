from beanie import Document
from pydantic import BaseModel, AwareDatetime, EmailStr
from beanie import PydanticObjectId


class AuthModel_DB(Document):
    username: str
    hashed_password: str
    email: EmailStr

class AuthModel_Output(BaseModel):
    username: str
    email: EmailStr

class AuthModel_External(BaseModel):
    username: str
    email: EmailStr

class RegisterModel(BaseModel):
    username: str
    password: str
    email: EmailStr

class UserNameSuggest_Input(BaseModel):
    username: str

class AuthModel(BaseModel):
    username: str
    password: str

class Token(BaseModel):
    access_token: str
    refresh_token: str


class TokenData(BaseModel):
    exp: AwareDatetime
    id: PydanticObjectId