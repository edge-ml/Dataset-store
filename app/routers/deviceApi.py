import os
from fastapi import APIRouter, UploadFile, File, Form, Response, BackgroundTasks
from starlette.background import BackgroundTask
from fastapi.param_functions import Depends
from fastapi.responses import FileResponse
from utils.json_encoder import JSONEncoder
from controller.dataset_controller import DatasetController
from controller.labelingController import getProjectLabelings
from routers.dependencies import validateApiKey
from typing import List, Dict, Optional
from pydantic import BaseModel
from controller.api_controller import initDataset, appendDataset
from typing import Tuple
import traceback
from utils.InMemoryLockManager import thread_safe
from controller.csv_uploadController import registerDownload, get_status

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
    labeling: InitDatasetModalLabeling | None = None



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

# Async upload
@router.post("/async/device/{api_key}")
async def upload_files_async(background_tasks: BackgroundTasks, json_data = Form(...), files: List[UploadFile] = File(...), apiData=Depends(validateApiKey('write'))):
    fileInfo = json.loads(json_data)
    userId = apiData["userId"]
    projectId = apiData["projectId"]
    downloadId = registerDownload(fileInfo, files, projectId, userId, background_tasks)
    headers = {"Location": str(downloadId)}
    return Response(status_code=202, content=json.dumps({"uploadId": downloadId}, cls=JSONEncoder), headers=headers)

@router.get("/async/device/{api_key}/status/{id}")
async def get_async_upload_status(id, apiData=Depends(validateApiKey('write'))):
    userId = apiData["userId"]
    status = get_status(id, userId)
    headers = {"X-status": str(status["status"])}
    return Response(json.dumps(status, cls=JSONEncoder, default=str), headers=headers)

def cleanUp(fileName):
    os.remove(fileName)

@router.get("/project/{api_key}/{dataset_id}/{timeseries_id}")
async def get_dataset_data(dataset_id, timeseries_id, response: Response, apiData=Depends(validateApiKey('read'))):
    projectId = apiData["projectId"]
    dataName = ctrl.getTimeSeriesData(projectId, dataset_id, timeseries_id)
    return FileResponse(dataName, media_type="application/octet-stream", filename="data.h5", background=BackgroundTask(cleanUp, fileName=dataName))
    

@router.get("/project/{api_key}")
async def get_project(apiData=Depends(validateApiKey('read'))):
    try:
        projectId = apiData["projectId"]
        datasets = ctrl.getDatasetInProject(projectId, includeTimeseriesData=False)
        labelings = getProjectLabelings(projectId)
        return Response(json.dumps({"datasets": datasets, "labelings": labelings}, cls=JSONEncoder), media_type="application/json")
    except Exception as e:
        print(e)
        print(traceback.format_exc())

