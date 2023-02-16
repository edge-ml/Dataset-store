from fastapi import APIRouter, Request, Header, Response, UploadFile, File, WebSocket, WebSocketDisconnect
from fastapi.param_functions import Depends

from app.controller.dataset_controller import DatasetController
from app.utils.json_encoder import JSONEncoder
from app.routers.dependencies import validateApiKey
from typing import List, Dict
from pydantic import BaseModel

import json

router = APIRouter()

ctrl = DatasetController()

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

@router.websocket("/csvws/{api_key}")
async def upload_ws(websocket: WebSocket , apiData=Depends(validateApiKey)):
    userId = apiData["userId"]
    projectId = apiData["projectId"]
    try:
        await websocket.accept()
        command = await websocket.receive_text()
        if command == "upload":
            await ctrl.uploadDatasetDevice(websocket, projectId, userId)
        print("Closing websocket")
        await websocket.close()
                    
    except WebSocketDisconnect:
        print("Websocket disconnected")

