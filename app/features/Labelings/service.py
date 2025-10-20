from app.features.Auth.models import AuthModel_DB
from app.features.Projects.models import ProjectModel_DB
from app.features.Labelings.models import LabelingModel_DB, LabelingModel_Input

async def get_project_labelings(auth_user: AuthModel_DB, project: ProjectModel_DB):
    return await LabelingModel_DB.find(LabelingModel_DB.project.id == project.id).to_list()


async def create_labeling(auth_user: AuthModel_DB, project: ProjectModel_DB, data: LabelingModel_Input):
    labeling = LabelingModel_DB(**data.dict(), project=project)
    await labeling.insert()
    return labeling