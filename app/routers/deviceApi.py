import os
from fastapi import APIRouter, UploadFile, File, Form, Response
from starlette.background import BackgroundTask
from fastapi.param_functions import Depends
from fastapi.responses import FileResponse
from app.utils.json_encoder import JSONEncoder
from app.controller.dataset_controller import DatasetController
from app.controller.labelingController import getProjectLabelings
from app.routers.dependencies import validateApiKey
from typing import List, Dict, Optional
from pydantic import BaseModel
from app.controller.api_controller import initDataset, appendDataset
from typing import Tuple
import traceback
from app.utils.InMemoryLockManager import thread_safe

import json

router = APIRouter()

ctrl = DatasetController()

class InitDatasetModalLabeling(BaseModel):
    labelingName: str
    labelName: str

class InitDatasetModal(BaseModel):
    name: str
    timeSeries: List[str]
    metaData: Dict[str,str]


class TimeSeriesDataModel(BaseModel):
    name: str
    data: List[Tuple[int, float]]

class IncrementUploadModal(BaseModel):
    data: List[TimeSeriesDataModel]    
    labeling: Optional[InitDatasetModalLabeling]



# Upload datasets in increments
@router.post("/dataset/init/{api_key}")
async def init_dataset(body: InitDatasetModal, apiData=Depends(validateApiKey('write'))):
    userId = apiData["userId"]
    projectId = apiData["projectId"]
    return {"id": initDataset(body.name, body.timeSeries, body.metaData, userId, projectId)}

@router.post("/dataset/append/{api_key}/{dataset_id}")
async def append_dataset(dataset_id, body: IncrementUploadModal, apiData=Depends(validateApiKey('write'))):
    try:
        with thread_safe(dataset_id):
            userId = apiData["userId"]
            projectId = apiData["projectId"]
            appendDataset(body, userId, projectId, dataset_id)
    except Exception as e:
        print(e)
        print(traceback.format_exc())

# Upload CSV-Files
@router.post("/dataset/device/{api_key}")
async def upload_files(json_data = Form(...), files: List[UploadFile] = File(...), apiData=Depends(validateApiKey('write'))):
    fileInfo = json.loads(json_data)
    userId = apiData["userId"]
    projectId = apiData["projectId"]
    await ctrl.uploadDatasetDevice(fileInfo, files, projectId, userId)
    
@router.get("/datasets/{api_key}")
async def get_datasets(includeTimeseriesData: bool, apiData=Depends(validateApiKey('read'))):
    projectId = apiData["projectId"]
    datasets = ctrl.getDatasetInProject(projectId, includeTimeseriesData=includeTimeseriesData)
    return Response(json.dumps(datasets, cls=JSONEncoder), media_type="application/json")


def cleanUp(fileName):
    os.remove(fileName)

@router.get("/project/{api_key}/{dataset_id}/{timeseries_id}")
async def get_dataset_data(dataset_id, timeseries_id, response: Response, apiData=Depends(validateApiKey('read'))):
    print("Getting timeseries")
    projectId = apiData["projectId"]
    dataName = ctrl.getTimeSeriesData(projectId, dataset_id, timeseries_id)
    return FileResponse(dataName, media_type="application/octet-stream", filename="data.h5", background=BackgroundTask(cleanUp, fileName=dataName))
    

@router.get("/project/{api_key}")
async def get_project(apiData=Depends(validateApiKey('read'))):
    projectId = apiData["projectId"]
    datasets = ctrl.getDatasetInProject(projectId, includeTimeseriesData=False)
    labelings = getProjectLabelings(projectId)
    return Response(json.dumps({"datasets": datasets, "labelings": labelings}, cls=JSONEncoder), media_type="application/json")

