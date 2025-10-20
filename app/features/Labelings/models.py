from beanie import Document, PydanticObjectId, Link
from pydantic import BaseModel, Field
from typing import List
from app.features.Projects.models import ProjectModel_DB

class LabelModel(BaseModel):
    name: str
    color: str
    id: PydanticObjectId = Field(default_factory=PydanticObjectId, alias="_id")

class LabelingModel_DB(Document):
    name: str
    labels: List[LabelModel]
    project: Link[ProjectModel_DB]

class LabelingModel_Input(BaseModel):
    name: str
    labels: List[LabelModel]