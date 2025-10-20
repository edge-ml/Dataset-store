from fastapi import APIRouter, Depends, Query, Header, Request, Response, Form
from app.utils.jwt import verify_token
from fastapi import UploadFile, File
from app.utils.project_header import project_header
from app.features.Datasets.models import DatasetModel_Input
from app.features.Datasets.service import get_all_datasets, create_dataset, get_single_dataset, getDataSetByIdStartEnd, getDatasetTimeSeriesStartEnd, getDatasetInProjectWithPagination, create_new_csv_dataset
import orjson
import rich

router = APIRouter()


@router.get("/view")
async def get_dataset_with_pagination(
    skip: int = Query(0, description="Skip number", ge=0),
    limit: int = Query(5, description="Limit number", ge=1),
    sort: str = Query('alphaAsc', description="Sorting algorithm"),
    project: str = Header(...),
    user_data=Depends(verify_token),
):
    return await getDatasetInProjectWithPagination(user_data, project, skip, limit, sort)

@router.get("/")
async def get_datasets_metadata(project = Depends(project_header), auth_user=Depends(verify_token)):
    return await get_all_datasets(auth_user=auth_user, project=project)

@router.post("/", status_code=201)
async def create_new_dataset(data: DatasetModel_Input, project = Depends(project_header), auth_user=Depends(verify_token)):
    return await create_dataset(auth_user=auth_user, project=project, data=data)

@router.get("/{id}")
async def get_single_dataset_metadata(id, project = Depends(project_header), auth_user=Depends(verify_token)):
    return await get_single_dataset(auth_user, project, id)

@router.get("/{id}/ts/{start}/{end}/{max_resolution}")
async def get_all_time_series_partially(id, start, end, max_resolution, project = Depends(project_header), auth_user=Depends(verify_token)):
    dataset = await getDataSetByIdStartEnd(id, project, start, end, max_resolution)
    res = orjson.dumps(dataset, option = orjson.OPT_SERIALIZE_NUMPY)
    return Response(res, media_type="application/json")

@router.get("/{id}/ts/{ts_id}/{start}/{end}/{max_resolution}")
async def get_time_series_partially(id, ts_id, start, end, max_resolution, project = Depends(project_header), user_data=Depends(verify_token)):
    timeSeries = await getDatasetTimeSeriesStartEnd(id, ts_id, project, start, end, max_resolution)
    res = orjson.dumps(timeSeries, option = orjson.OPT_SERIALIZE_NUMPY)
    return Response(res, media_type="application/json")


@router.post("/csv", status_code=201)
async def create_new_dataset_from_csv(CSVFile: UploadFile = File(...), CSVConfig: str = Form(...), project = Depends(project_header), auth_user=Depends(verify_token)):
    return await create_new_csv_dataset(CSVFile, CSVConfig, project, auth_user)