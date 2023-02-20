from fastapi import APIRouter
from app.routers import dataset, deviceApi, label, labelings, csv

router = APIRouter()
router.include_router(
    dataset.router,
    prefix='/dataset',
)

router.include_router(
    label.router,
    prefix='/label',
)

router.include_router(
    deviceApi.router,
    prefix="/api"
)

router.include_router(
    labelings.router,
    prefix="/labelings"
)

router.include_router(
    csv.router,
    prefix="/csv"
)