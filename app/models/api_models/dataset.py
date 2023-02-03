from pydantic import BaseModel, Field
from typing import List, Dict, Union, Optional
from .timeseries import TimeSeries
from .label import LabelingsObject

class Dataset(BaseModel):
    # id: str = Optional[Field(..., alias="_id")]
    timeSeries: List[TimeSeries]
    # projectId: str = Field(..., alias="projectId")
    metaData: Optional[Dict[str, str]] = {}
    name: str
    labelings: Optional[List[LabelingsObject]] = []
    canEdit: Optional[bool] = False
    
