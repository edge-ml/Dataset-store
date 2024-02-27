from fastapi import APIRouter
from routers import dataset, deviceApi, label, labelings, csv, timeSeries

router = APIRouter()

router.include_router(
    csv.router,
    prefix="/download"
)

router.include_router(
    dataset.router,
    prefix='/datasets',
    tags=["datasets"]
)

router.include_router(
    timeSeries.router,
    prefix="/dataset/timeSeries",
    tags=["TimeSeries"]
)

router.include_router(
    label.router,
    prefix='/datasets/labelings',
)

router.include_router(
    deviceApi.router,
    prefix="/api"
)

router.include_router(
    labelings.router,
    prefix="/labelings"
)