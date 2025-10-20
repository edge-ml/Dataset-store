from app.features.Auth.models import AuthModel_DB
from app.features.Projects.models import ProjectModel_DB, ProjectModel_Input
from beanie.operators import Or
import rich

async def list_all_projects(auth_user: AuthModel_DB):
    projects = await ProjectModel_DB.find(Or(ProjectModel_DB.admin.id == auth_user.id,
                                             ProjectModel_DB.users.id == auth_user.id), fetch_links=True).to_list()

    return projects

async def create_project(auth_user: AuthModel_DB, project_data: ProjectModel_Input):
    # Check if there is already a project with the same name for this admin
    existing_project = await ProjectModel_DB.find_one(
        ProjectModel_DB.name == project_data.name,
        ProjectModel_DB.admin.id == auth_user.id
    )
    if existing_project:
        raise Exception("Project with the same name already exists for this admin")

    new_project = ProjectModel_DB(**project_data.dict(), admin=auth_user)
    await new_project.insert()
    return new_project