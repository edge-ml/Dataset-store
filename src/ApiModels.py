from pydantic import BaseModel, Field
from typing import List, Dict, Union, Optional

class TimeSeries(BaseModel):
    _id: str = Field(alias="_id", default=None)
    name: str
    unit: str
    start: int
    end: int
    data: List[List[float]]

class Label(BaseModel):
    type: str
    start: int
    end: int


class LabelingsObject(BaseModel):
    labelingId: str =  Field(..., alias="_id")
    labels: List[Label]


class Dataset(BaseModel):
    # id: str = Optional[Field(..., alias="_id")]
    timeSeries: List[TimeSeries]
    # projectId: str = Field(..., alias="projectId")
    metaData: Optional[Dict[str, str]] = {}
    name: str
    labelings: Optional[List[LabelingsObject]] = []
    canEdit: Optional[bool] = False
    
