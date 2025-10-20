from fastapi import Header, HTTPException, Depends
from app.features.Projects.models import ProjectModel_DB
from app.features.Auth.models import AuthModel_DB
from app.utils.jwt import verify_token
from beanie import PydanticObjectId
from beanie.operators import Or, And


async def project_header(
    auth_user: AuthModel_DB = Depends(verify_token),
    project: PydanticObjectId = Header(...),
):
    project_id = PydanticObjectId(project)

    project = await ProjectModel_DB.find_one(
        And(
            ProjectModel_DB.id == project_id,
            Or(
                ProjectModel_DB.admin.id == auth_user.id,
                ProjectModel_DB.users.id == auth_user.id
            )
        )
    )

    if not project:
        raise HTTPException(status_code=403, detail="You do not have access to this project")

    return project