from pydantic import BaseModel, ValidationError, validator, Field
from typing import Any, Dict, List, Optional, Union
from enum import Enum
import re
from beanie import Document, PydanticObjectId, Link
from app.features.Projects.models import ProjectModel_DB

class ProgressStep(Enum):
    PARSING = ["Parsing the file", 20]
    LABELING = ["Extracting labels", 40]
    CREATING_DATASET = ["Creating dataset", 60]
    UPLOADING_DATASET = ["Syncing Timeseries with DB", 80]
    COMPLETE = ["Complete", 100]
    
class SamplingRate(BaseModel):
    mean: float
    var: float

class TimeSeries(BaseModel):
    id: PydanticObjectId = Field(default_factory=PydanticObjectId, alias="_id")
    start: int | None = None
    end: int | None = None
    unit: str = Field(default="")
    name: str
    samplingRate: SamplingRate | None = None
    length: int | None = None

class TimeSeriesInput(BaseModel):
    name: str
    unit: str | None = None
    data: Any


class DatasetLabel(BaseModel):
    start: int
    end: int
    type: PydanticObjectId = Field(default_factory=PydanticObjectId)
    id: PydanticObjectId = Field(default_factory=PydanticObjectId, alias="_id")
    metaData: Dict[str, str] = Field(default={})

    @validator('end')
    def check_start_end(cls, v, values):
        if 'start' in values and v <= values['start']:
            raise ValueError('end must be strictly greater than start')
        return v

class DatasetLabeling(BaseModel):
    labelingId: PydanticObjectId = Field(default_factory=PydanticObjectId)
    labels: List[DatasetLabel]

class DatasetModel_DB(Document):
    project: Link[ProjectModel_DB]
    name: str
    metaData: Dict[str, str] = Field(default={})
    timeSeries: List[TimeSeries] = Field(default=[])
    labelings: List[DatasetLabeling] = Field(default=[])


class DatasetModel_Input(BaseModel):
    name: str
    metaData: Dict[str, str] = Field(default={})
    timeSeries: List[TimeSeriesInput] = Field(default=[])
    labelings: List[DatasetLabeling] = Field(default=[])
    