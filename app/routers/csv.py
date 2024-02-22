from fastapi import APIRouter, Header, Response
from fastapi.param_functions import Depends
from utils.json_encoder import JSONEncoder
from routers.dependencies import validate_user
from fastapi import BackgroundTasks
import json
from controller.downloadController import registerForDownloadDataset, get_status, get_download_data, cancel_download, registerForDownloadProject
from utils.helpers import PyObjectId

requests = {}

from controller.dataset_controller import DatasetController

router = APIRouter()
ctrl = DatasetController()

@router.get("/status/")
async def Check_download_status(user_data=Depends(validate_user)):
    res =  await get_status(user_data[0])
    res_dict = [x.dict() for x in res]
    return Response(json.dumps(res_dict, cls=JSONEncoder, default=str), media_type="application/json")

@router.get("/{id}")
async def Download_resource(id):
    return await get_download_data(id)

@router.post("/dataset/{datasetId}")
async def Register_dataset_for_download(datasetId, background_tasks: BackgroundTasks, project: str = Header(...), user_data=Depends(validate_user)):
    data = registerForDownloadDataset(project, datasetId, user_data[0], background_tasks)
    return Response(json.dumps(data, cls=JSONEncoder, default=str), media_type="application/json")

@router.post("/project")
async def Register_project_for_download(background_tasks: BackgroundTasks, project: PyObjectId = Header(...), user_data=Depends(validate_user)):
    data = registerForDownloadProject(project, user_data[0], background_tasks)
    return Response(json.dumps(data, cls=JSONEncoder, default=str), media_type="application/json")


@router.delete("/{downloadId}")
async def Delete_download(downloadId, user_data=Depends(validate_user)):
    await cancel_download(downloadId)
    return Response(status_code=200)