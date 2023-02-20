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
import cachetools

requests = {}

from app.controller.dataset_controller import DatasetController
from app.routers.schema import DatasetSchema

router = APIRouter()
ctrl = DatasetController()

class DatasetIdList(BaseModel):
    datasets: List[str]

@router.post("/")
async def get_multiple_csv_datast(body: DatasetIdList, project: str = Header(), user_data=Depends(validate_user)):
    id = "%06x" % random.randint(0, 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF) 
    requests[id] = [project, body.datasets]
    return {"id": id}

@router.get("/download/{id}")
async def download_csv(id):
    [project, ids] = requests[id]
    if len(ids) > 1:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "a",
                     zipfile.ZIP_DEFLATED, False) as file:
            for id in ids:
                fileCSV, fileName = await ctrl.getCSV(project, id)
                file.writestr(fileName, fileCSV.getvalue())
        response = StreamingResponse(iter([zip_buffer.getvalue()]), media_type="application/zip")
        response.headers["Content-Disposition"] = f"attachment; filename=all.zip"
        return response
    else:
        file, fileName = await ctrl.getCSV(project, ids[0])
        response = StreamingResponse(iter([file.getvalue()]), media_type="text/csv")
        response.headers["Content-Disposition"] = f"attachment; filename={fileName}.csv"
        return response
