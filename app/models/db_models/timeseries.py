from pydantic import BaseModel, Field
import uuid

class TimeSeriesModel(BaseModel):
    id: str = Field(default_factory=uuid.uuid4, alias="_id")
    projectId: str = Field(default_factory=uuid.uuid4)
    datasetId: str = Field(default_factory=uuid.uuid4)
    name: str = Field(...)
    unit: str = Field(...)
    start: int = Field(...)
    end: int = Field(...)
    sampingRate: int = Field(...)
