from fastapi import APIRouter, HTTPException, status, Request, Header, Response, Form, File, UploadFile
from fastapi.param_functions import Depends, Query
from app.utils.json_encoder import JSONEncoder
from app.utils.CsvParser import CsvParser
from app.routers.dependencies import validate_user
from app.utils.helpers import PyObjectId
import traceback
import json
import orjson
from typing import List

from app.controller.dataset_controller import DatasetController

from app.routers.schema import DatasetSchema

router = APIRouter()

ctrl = DatasetController()


# Create dataset
@router.post("/")
async def createDataset(body: Request, project: str = Header(...), user_data=Depends(validate_user)):
    body = await body.json()
    # body = body.dict(by_alias=True)
    (user_id, _, _) = user_data
    ctrl.addDataset(dataset=body, project=project, user_id=user_id)
    return {"message": "success"}

# Create dataset from csv file
# TODO: handle labels
@router.post("/create")
async def createDataset(CSVFile: UploadFile = File(...), CSVConfig: str = Form(...), project: str = Header(...), user_data = Depends(validate_user)):
    (user_id, _, _) = user_data
    metadata = None
    config = json.loads(CSVConfig)
    try:
        metadata = ctrl.CSVUpload(CSVFile, config, project, user_id)
    except Exception as exp:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Error while creating the dataset")
    return Response(json.dumps(metadata, cls=JSONEncoder), media_type="application/json")

@router.get("/view")
async def getDatasetsInProjectWithPagination(
    project: str = Header(...),
    user_data=Depends(validate_user),
    page: int = Query(1, description="Page number", ge=1),
    page_size: int = Query(5, description="Page size", ge=5),
    sort: str = Query('alphaAsc', description="Sorting algorithm")
):
    datasets, total_count = ctrl.getDatasetInProjectWithPagination(project, page, page_size, sort)
    response_data = {
        "datasets": datasets,
        "total_datasets": total_count
    }

    return Response(json.dumps(response_data, cls=JSONEncoder), media_type="application/json")

# Get metadata of dataset
@router.get("/{id}")
async def getDatasetMetaData(id, project : str = Header(...), user_data=Depends(validate_user)):
    dataset = ctrl.getDatasetById(id, project, onlyMeta=True)
    return Response(json.dumps(dataset, cls=JSONEncoder), media_type="application/json")


@router.get("/")
async def getDatasetsInProject(project: str = Header(...), user_data=Depends(validate_user)):
    data = ctrl.getDatasetInProject(project)
    return Response(json.dumps(data, cls=JSONEncoder), media_type="application/json")


# Get dataset
# @router.get("/{id}")
# async def getDataset(id, project: str = Header(...), user_data=Depends(validate_user)):
#     dataset = ctrl.getDatasetById(id, project)
#     return Response(json.dumps(dataset, cls=JSONEncoder), media_type="application/json")

@router.post("/{id}/ts/{start}/{end}/{max_resolution}")
async def getTimeSeriesDatasetPartial(id, start, end, max_resolution, body: List[str], project: str = Header(...), user_data=Depends(validate_user)):
    timeSeries = ctrl.getDatasetTimeSeriesStartEnd(id, body, project, start, end, max_resolution)
    res = orjson.dumps(timeSeries, option = orjson.OPT_SERIALIZE_NUMPY)
    return Response(res, media_type="application/json")


@router.get("/{id}/ts/{start}/{end}/{max_resolution}")
async def getTimeSeriesDataset(id, start, end, max_resolution, project: str = Header(...), user_data=Depends(validate_user)):
    dataset = ctrl.getDataSetByIdStartEnd(id, project, start, end, max_resolution)
    res = orjson.dumps(dataset, option = orjson.OPT_SERIALIZE_NUMPY)
    return Response(res, media_type="application/json")


# Delete dataset
@router.delete("/{id}")
async def deleteDatasetById(id, project: str = Header(...), user_data=Depends(validate_user)):
    ctrl.deleteDataset(id, projectId=project)

# Update dataset
@router.put("/{id}")
async def updateDatasetById(id, body: Request, project: str = Header(...), user_data=Depends(validate_user)):
    body = await body.json()
    body['projectId'] = project
    body['userId'] = user_data[0]
    body['metaData'] = {}
    ctrl.updateDataset(id, project, body)
    return {"message": "success"}

@router.post("/{id}/append")
async def appendToDataset(id, body: Request, project: str = Header(...), user_data=Depends(validate_user)):
    try:
        body = await body.json()
        ctrl.append(id, project, body, projectId=project)
    except Exception as e:
        print(e)
        print(traceback.format_exc())