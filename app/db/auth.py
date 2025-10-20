from beanie import Document
from pydantic import BaseModel


class AuthModel_DB(Document):
    username: str
    hashed_password: str
    email: str


class AuthModel_Input(BaseModel):
    username: str
    password: str
    email: str