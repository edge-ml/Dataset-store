from pydantic import BaseModel, Field
from typing import List

class Label(BaseModel):
    type: str
    start: int
    end: int

class LabelingsObject(BaseModel):
    labelingId: str =  Field(..., alias="_id")
    labels: List[Label]