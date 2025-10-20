from fastapi import APIRouter, Depends
from app.features.Auth.models import AuthModel_DB
from app.utils.jwt import verify_token
from app.features.Projects.service import list_all_projects
from app.features.Projects.models import ProjectModel_DB, ProjectModel_Input, ProjectModel_Output
from app.features.Projects.service import create_project

router = APIRouter()


@router.get("/", response_model=list[ProjectModel_Output])
async def get_all_projects(auth_user: AuthModel_DB = Depends(verify_token)):
    return await list_all_projects(auth_user)

@router.post("/", response_model=ProjectModel_Output, status_code=201)
async def create_new_project(project_data: ProjectModel_Input, auth_user: AuthModel_DB = Depends(verify_token)):
    return await create_project(auth_user, project_data)