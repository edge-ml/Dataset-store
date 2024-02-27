from pydantic import BaseModel, Field
from utils.helpers import PyObjectId
from typing import Dict, List



class UploadDatasetModel(BaseModel):
    pass


class SamplingRate(BaseModel):
    mean: float
    var: float


class TimeSeries(BaseModel):
    id: PyObjectId = Field(..., alias="_id")
    start: int
    end: int
    unit: str
    name: str
    samplingRate: SamplingRate
    length: int


class Label(BaseModel):
    start: int
    end: int
    type: PyObjectId
    id: PyObjectId = Field(..., alias="_id")
    metaData: Dict[str, str]


class Labeling(BaseModel):
    labelingId: PyObjectId
    labels: List[Label]


class ReturnDataset(BaseModel):
    id: PyObjectId = Field(..., alias="_id")
    projectId: PyObjectId
    name: str
    metaData: Dict[str, str]
    timeSeries: List[TimeSeries]
    labelings: List[Labeling]
    userId: PyObjectId
