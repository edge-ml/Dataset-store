from fastapi import APIRouter
from app.features import Auth, Projects, Datasets, Labelings

router = APIRouter()

router.include_router(Auth.router, prefix="/auth", tags=["Authentication"])
router.include_router(Projects.router, prefix="/projects", tags=["Projects"])
router.include_router(Datasets.router, prefix="/datasets", tags=["Datasets2"])
router.include_router(Labelings.router, prefix="/labelings", tags=["Labelings"])