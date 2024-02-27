from fastapi import APIRouter, Header, Response
from fastapi.param_functions import Depends
from routers.dependencies import validate_user
import orjson
from typing import List
from controller import DatasetController

# Typing
from models.api import TimeSeries

router = APIRouter()

ctrl = DatasetController()


@router.get("/{datasetId}/{timeSeriesId}/{start}/{end}/{max_resolution}", response_model=TimeSeries)
async def Get_timeSeries_partially(datasetId, timeSeriesId, start, end, max_resolution, project: str = Header(...), user_data=Depends(validate_user)):
    timeSeries = ctrl.getDataSetByIdStartEnd(datasetId, timeSeriesId, project, start, end, max_resolution)
    res = orjson.dumps(timeSeries, option = orjson.OPT_SERIALIZE_NUMPY)
    return Response(res, media_type="application/json")