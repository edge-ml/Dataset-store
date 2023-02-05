from fastapi import APIRouter, Request, Header, Response
from fastapi.param_functions import Depends

from app.models.api_models.timeseries import TimeSeries
from app.models.api_models.dataset import Dataset
from app.utils.json_encoder import JSONEncoder
from app.routers.dependencies import validate_user

import json
import orjson

from app.controller.dataset_controller import DatasetController

router = APIRouter()

ctrl = DatasetController()

# Create dataset
@router.post("/")
async def createDataset(body: Request, project: str = Header(), user_data=Depends(validate_user)):
    body = await body.json()
    ctrl.addDataset(dataset=body, project=project)
    return {"message": "success"}


@router.get("/project")
async def getDatasetsInProject(project: str = Header(), user_data=Depends(validate_user)):
    data = ctrl.getDatasetInProject(project)
    return Response(json.dumps(data, cls=JSONEncoder), media_type="application/json")

# Get dataset
@router.get("/{id}")
async def getDataset(id, project: str = Header(), user_data=Depends(validate_user)):
    dataset = ctrl.getDatasetById(id, project)
    return Response(json.dumps(dataset, cls=JSONEncoder), media_type="application/json")


@router.get("/{id}/ts/{start}/{end}/{max_resolution}")
async def getTimeSeriesDataset(id, start, end, max_resolution, project: str = Header(), user_data=Depends(validate_user)):
    dataset = ctrl.getDataSetByIdStartEnd(id, project, start, end, max_resolution)
    res = orjson.dumps(dataset, option = orjson.OPT_SERIALIZE_NUMPY)

    return Response(res, media_type="application/json")

# Get metadata of dataset
@router.get("/{id}/meta")
async def getDatasetMetaData(id, project : str = Header(), user_data=Depends(validate_user)):
    dataset = ctrl.getDatasetById(id, project, onlyMeta=True)
    return Response(json.dumps(dataset, cls=JSONEncoder), media_type="application/json")

# Delete dataset
@router.delete("/{id}")
async def deleteDatasetById(id, project: str = Header(), user_data=Depends(validate_user)):
    ctrl.deleteDataset(id, projectId=project)