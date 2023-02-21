from fastapi import APIRouter, Request, Header, Response
from fastapi.param_functions import Depends
from app.utils.json_encoder import JSONEncoder
from app.routers.dependencies import validate_user
from fastapi.responses import StreamingResponse, FileResponse
import random
import collections.abc
from pydantic import BaseModel
from typing import List
import json
import orjson
import io
import zipfile
from app.controller.downloadController import registerForDownload, download

requests = {}

from app.controller.dataset_controller import DatasetController
from app.routers.schema import DatasetSchema

router = APIRouter()
ctrl = DatasetController()

class DatasetIdList(BaseModel):
    datasets: List[str]

@router.post("/")
async def get_multiple_csv_datast(body: DatasetIdList, project: str = Header(), user_data=Depends(validate_user)):
    id = registerForDownload(project, body.datasets)
    return {"id": id}

@router.get("/download/{id}")
async def download_csv(id):
    return await download(id)
