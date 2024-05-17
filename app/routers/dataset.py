from fastapi import APIRouter, HTTPException, status, Request, Header, Response, Form, File, UploadFile
from fastapi.param_functions import Depends, Query
from utils.json_encoder import JSONEncoder
from utils.CsvParser import CsvParser
from routers.dependencies import validate_user
from utils.helpers import PyObjectId
import traceback
import json
import orjson
from typing import List

from controller.dataset_controller import DatasetController

from routers.schema import DatasetSchema

router = APIRouter()

ctrl = DatasetController()

@router.get("/")
async def Get_datasets_metadata(project: str = Header(...), user_data=Depends(validate_user)):
    data = ctrl.getDatasetInProject(project)
    return Response(json.dumps(data, cls=JSONEncoder), media_type="application/json")

@router.get("/{id}")
async def Get_single_dataset_metadata(id, project : str = Header(...), user_data=Depends(validate_user)):
    dataset = ctrl.getDatasetById(id, project, onlyMeta=True)
    return Response(json.dumps(dataset, cls=JSONEncoder), media_type="application/json")


@router.get("/{id}/ts/{start}/{end}/{max_resolution}")
async def Get_timeSeries_partially(id, start, end, max_resolution, project: str = Header(...), user_data=Depends(validate_user)):
    dataset = ctrl.getDataSetByIdStartEnd(id, project, start, end, max_resolution)
    res = orjson.dumps(dataset, option = orjson.OPT_SERIALIZE_NUMPY)
    return Response(res, media_type="application/json")

@router.post("/")
async def create_dataset(body: Request, project: str = Header(...), user_data=Depends(validate_user)):
    body = await body.json()
    (user_id, _, _) = user_data
    ctrl.addDataset(dataset=body, project=project, user_id=user_id)
    return {"message": "success"}



@router.post("/create")
async def create_dataset_with_csv(CSVFile: UploadFile = File(...), CSVConfig: str = Form(...), project: str = Header(...), user_data = Depends(validate_user)):
    (user_id, _, _) = user_data
    metadata = None
    config = json.loads(CSVConfig)
    try:
        metadata = ctrl.CSVUpload(CSVFile, config, project, user_id)
    except Exception as exp:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Error while creating the dataset")
    return Response(json.dumps(metadata, cls=JSONEncoder), media_type="application/json")

@router.post("/view")
async def get_dataset_with_pagination(
    request: Request,
    page: int = Query(1, description="Page number", ge=1),
    page_size: int = Query(5, description="Page size", ge=5),
    sort: str = Query('alphaAsc', description="Sorting algorithm"),
    project: str = Header(...),
    user_data=Depends(validate_user),
):
    body = await request.json()
    print(body)
    datasets, total_count = ctrl.getDatasetInProjectWithPagination(project, page, page_size, sort, body.get('filters', {}))
    response_data = {
        "datasets": datasets,
        "total_datasets": total_count
    }
    return Response(json.dumps(response_data, cls=JSONEncoder), media_type="application/json")


@router.get("/{id}/ts/{ts_id}/{start}/{end}/{max_resolution}")
async def get_time_series_partially(id, ts_id, start, end, max_resolution, project: str = Header(...), user_data=Depends(validate_user)):
    timeSeries = ctrl.getDatasetTimeSeriesStartEnd(id, ts_id, project, start, end, max_resolution)
    res = orjson.dumps(timeSeries, option = orjson.OPT_SERIALIZE_NUMPY)
    return Response(res, media_type="application/json")

# Update dataset
@router.put("/{id}")
async def update_dataset_by_id(id, body: Request, project: str = Header(...), user_data=Depends(validate_user)):
    body = await body.json()
    ctrl.updateDataset(id, project, body)
    return {"message": "success"}

@router.post("/{id}/append")
async def append_to_dataset(id, body: Request, project: str = Header(...), user_data=Depends(validate_user)):
    try:
        body = await body.json()
        ctrl.append(id, project, body, projectId=project)
    except Exception as e:
        print(e)
        print(traceback.format_exc())

@router.delete("/{id}")
async def deleteDatasetById(id, project: str = Header(...), user_data=Depends(validate_user)):
    ctrl.deleteDataset(id, projectId=project)
