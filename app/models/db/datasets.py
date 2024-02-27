from pydantic import BaseModel, Field
from typing import Dict, List, Union
from enum import Enum
from utils.helpers import PyObjectId
from bson.objectid import ObjectId



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
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    start: int | None = None
    end: int | None = None
    unit: str = Field(default="")
    name: str
    samplingRate: SamplingRate | None = None
    length: int | None = None


class DatasetLabel(BaseModel):
    start: int
    end: int
    type: PyObjectId = Field(default_factory=PyObjectId)
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    metaData: Dict[str, str] = Field(default={})

class DatasetLabeling(BaseModel):
    labelingId: PyObjectId = Field(default_factory=PyObjectId)
    labels: List[DatasetLabel]

class DatasetDBSchema(BaseModel):
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    projectId: PyObjectId
    name: str
    metaData: Dict[str, str] = Field(default={})
    timeSeries: List[TimeSeries] = Field(default=[])
    labelings: List[DatasetLabeling] = Field(default=[])
    userId: PyObjectId
    progressStep: List[Union[str, int]] = Field(default=ProgressStep.PARSING.value)