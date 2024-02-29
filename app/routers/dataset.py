from fastapi import APIRouter, HTTPException, status, Request, Header, Response, Form, File, UploadFile
from fastapi.param_functions import Depends, Query
from utils.json_encoder import JSONEncoder
from routers.dependencies import validate_user
import traceback
from typing import List
import json
from utils.helpers import PyObjectId


# Types
from models.api import ReturnDataset
from models.db import DatasetDBSchema

from controller.dataset_controller import DatasetController


router = APIRouter()

ctrl = DatasetController()




@router.get("/", response_model=List[ReturnDataset])
async def Get_datasets_metadata(project: str = Header(...), user_data=Depends(validate_user)):
    datasets : List[DatasetDBSchema] = ctrl.getDatasetInProject(project)
    datasets : List[ReturnDataset] = [ReturnDataset(**x.model_dump(by_alias=True)) for x in datasets]
    return datasets

@router.get("/{id}", response_model=ReturnDataset)
async def Get_single_dataset_metadata(id, project : str = Header(...), user_data=Depends(validate_user)):
    dataset: ReturnDataset = ctrl.getDatasetById(id, project)
    dataset = ReturnDataset(**dataset.model_dump(by_alias=True))
    return dataset

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


@router.delete("/{id}", response_model=bool)
async def deleteDatasetById(id : PyObjectId, project: str = Header(...), user_data=Depends(validate_user)):
    return ctrl.deleteDataset(id, projectId=project)

