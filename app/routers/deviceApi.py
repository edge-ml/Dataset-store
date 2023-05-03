from fastapi import APIRouter, Request, Header, Response, UploadFile, File, WebSocket, WebSocketDisconnect, Form
from fastapi.param_functions import Depends

from app.controller.dataset_controller import DatasetController
from app.utils.json_encoder import JSONEncoder
from app.routers.dependencies import validateApiKey
from typing import List, Dict, Optional
from pydantic import BaseModel, conlist
from app.controller.api_controller import initDataset, appendDataset
from typing import Union, Tuple
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


@router.post("/csv")
async def create_upload_files(files: List[UploadFile]):
    print("file upload")
    await ctrl.processFiles(files)
    return {"filenames": [file.filename for file in files]}


@router.post("/dataset/init/{api_key}")
async def init_dataset(body:InitDatasetModal, apiData=Depends(validateApiKey)):
    userId = apiData["userId"]
    projectId = apiData["projectId"]
    return {"id": initDataset(body.name, body.timeSeries, body.metaData, userId, projectId)}

@router.post("/dataset/append/{api_key}/{dataset_id}")
async def append_dataset(dataset_id, body: IncrementUploadModal, apiData=Depends(validateApiKey)):
    try:
        with thread_safe(dataset_id):
            userId = apiData["userId"]
            projectId = apiData["projectId"]
            appendDataset(body, userId, projectId, dataset_id)
    except Exception as e:
        print(e)
        print(traceback.format_exc())



@router.websocket("/csvws/{api_key}")
async def upload_ws(websocket: WebSocket , apiData=Depends(validateApiKey)):
    userId = apiData["userId"]
    projectId = apiData["projectId"]
    print(userId, projectId)
    try:
        await websocket.accept()
        command = await websocket.receive_text()
        print(command)
        if command == "upload":
            await ctrl.uploadDatasetDevice(websocket, projectId, userId)
        print("Closing websocket")
        await websocket.close()
                    
    except WebSocketDisconnect:
        print("Websocket disconnected")
    
    except Exception as e:
        print(e)
        print(traceback.format_exc())

@router.post("/dataset/device/{api_key}")
async def upload_files(json_data = Form(...), files: List[UploadFile] = File(...), apiData=Depends(validateApiKey)):
    fileInfo = json.loads(json_data)
    userId = apiData["userId"]
    projectId = apiData["projectId"]
    await ctrl.uploadDatasetDevice(fileInfo, files, projectId, userId)