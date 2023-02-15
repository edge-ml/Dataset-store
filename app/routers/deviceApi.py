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

class FileDescriptor(BaseModel):
    name: str
    size: int
    drop: List[str]
    time: str

class CSVLabel(BaseModel):
    start: str
    end: str
    name: str
    metaData: Dict[str, str]

class CsvLabeling(BaseModel):
    name: str
    labels: List[CSVLabel]

class CSVDatasetInfo(BaseModel):
    name: str
    files: List[FileDescriptor]
    labeling: CsvLabeling

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
    try:
        quit = False
        await websocket.accept()
        while not quit:
            command = await websocket.receive_text()
            if command == "upload":
                transmitting = True
                while transmitting:
                    info = await websocket.receive_json(mode="text")
                    info = CSVDatasetInfo.parse_obj(info).dict(by_alias=True)
                    print(info)
                    total_size = sum([x["size"] for x in info["files"]])
                    print(total_size)
                    bytes = bytearray()
                    while True:
                        data = await websocket.receive_bytes()
                        bytes += data
                        if len(bytes) == total_size:
                            print("Transmission complete", apiData["projectId"], apiData["userId"])
                            ctrl.uploadDatasetDevice(info, bytes, projectId=apiData["projectId"] , userId=apiData["userId"])
                            transmitting = False
                            break
                    
    except WebSocketDisconnect:
        print("Websocket disconnected")

