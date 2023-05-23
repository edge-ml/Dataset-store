from fastapi import APIRouter, Request, Header, Response
from fastapi.param_functions import Depends
from app.utils.json_encoder import JSONEncoder
from app.routers.dependencies import validate_user
from fastapi.responses import StreamingResponse, FileResponse
from fastapi import BackgroundTasks
import random
import collections.abc
from pydantic import BaseModel
from typing import List
import json
import orjson
import io
import zipfile
from app.controller.downloadController import registerForDownloadDataset, get_status, get_download_data, cancel_download, registerForDownloadProject
from app.utils.helpers import PyObjectId

requests = {}

from app.controller.dataset_controller import DatasetController
from app.routers.schema import DatasetSchema

router = APIRouter()
ctrl = DatasetController()

@router.post("/dataset/{dataset_id}")
async def get_multiple_csv_datast(dataset_id, background_tasks: BackgroundTasks, project: str = Header(...), user_data=Depends(validate_user)):
    data = registerForDownloadDataset(project, dataset_id, user_data[0], background_tasks)
    return Response(json.dumps(data, cls=JSONEncoder, default=str), media_type="application/json")

@router.post("/project")
async def download_project(background_tasks: BackgroundTasks, project: PyObjectId = Header(...), user_data=Depends(validate_user)):
    data = registerForDownloadProject(project, user_data[0], background_tasks)
    return Response(json.dumps(data, cls=JSONEncoder, default=str), media_type="application/json")

@router.get("/status/")
async def download_csv(user_data=Depends(validate_user)):
    res =  await get_status(user_data[0])
    res_dict = [x.dict() for x in res]
    return Response(json.dumps(res_dict, cls=JSONEncoder, default=str), media_type="application/json")

@router.delete("/{downloadId}")
async def del_download(downloadId, user_data=Depends(validate_user)):
    await cancel_download(downloadId)
    return Response(status_code=200)

@router.get("/{id}")
async def download_csv(id):
    return await get_download_data(id)