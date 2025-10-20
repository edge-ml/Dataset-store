from beanie import Document, PydanticObjectId, Link
from typing import List
from pydantic import Field, BaseModel
from app.features.Auth.models import AuthModel_External

from app.features.Auth.models import AuthModel_DB


class ProjectModel_DB(Document):
    admin: Link[AuthModel_DB] = Field(..., description="Admin user ID")
    name: str = Field(..., description="Project name")
    users: List[Link[AuthModel_DB]] = Field(default_factory=list, description="List of project users")
    enable_device_api: bool = Field(default=False, description="Enable device API")


class ProjectModel_Input(BaseModel):
    name: str = Field(..., description="Project name")

class ProjectModel_Output(BaseModel):
    id: PydanticObjectId = Field(..., description="Project ID")
    name: str = Field(..., description="Project name")
    admin: AuthModel_External = Field(..., description="Admin user IDs")
    users: List[Link[PydanticObjectId]] = Field(..., description="List of project user IDs")
    enable_device_api: bool = Field(..., description="Enable device API")