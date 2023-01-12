from pydantic import BaseModel, Field
from typing import List, Dict, Union

class TimeSeries(BaseModel):
    id: str = Field(..., alias="_id")
    name: str
    unit: str
    start: int
    end: int
    data: List[List[float]]
