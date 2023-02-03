from fastapi import APIRouter
from app.routers import dataset, label, store

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
    store.router,
)
