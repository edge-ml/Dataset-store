from fastapi import APIRouter
from app.routers import dataset, deviceApi, label, labelings

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