from fastapi import APIRouter, Request, Header, Response, UploadFile, File, WebSocket, WebSocketDisconnect, Form
from fastapi.param_functions import Depends

from app.controller.dataset_controller import DatasetController
from app.utils.json_encoder import JSONEncoder
from app.routers.dependencies import validateApiKey
from typing import List, Dict
from pydantic import BaseModel, conlist
from app.controller.api_controller import initDataset, appendDataset
from typing import Union
import traceback

import json

router = APIRouter()

ctrl = DatasetController()

class InitDatasetModal(BaseModel):
    name: str
    timeSeries: List[str]
    metaData: Dict[str,str]


class TimeSeriesDataModel(BaseModel):
    name: str
    data: List[conlist(Union[int, float], max_items=2, min_items=2)]

@router.post("/csv")
async def create_upload_files(files: List[UploadFile]):
    print("file upload")
    await ctrl.processFiles(files)
    return {"filenames": [file.filename for file in files]}

# @router.post("/key/{api_key}")
# async def externalUpload(api_key, body: Request, user_id=Depends(validateApiKey)):
#     body = await body.json()
#     print("body")
#     print(body)
#     res = ctrl.externalUpload(api_key, user_id, body)
#     return Response(json.dumps(res, cls=JSONEncoder), media_type="application/json")


@router.post("/dataset/init/{api_key}")
async def init_dataset(body:InitDatasetModal, apiData=Depends(validateApiKey)):
    userId = apiData["userId"]
    projectId = apiData["projectId"]
    return {"id": initDataset(body.name, body.timeSeries, body.metaData, userId, projectId)}

@router.post("/dataset/append/{api_key}")
async def append_dataset(body:TimeSeriesDataModel, apiData=Depends(validateApiKey)):
    userId = apiData["userId"]
    projectId = apiData["projectId"]
    appendDataset(body.name, body.timeSeries, userId, projectId)


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