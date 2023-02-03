from pydantic import BaseModel, Field
from typing import List

class TimeSeries(BaseModel):
    _id: str = Field(alias="_id", default=None)
    name: str
    unit: str
    start: int
    end: int
    data: List[List[float]]
