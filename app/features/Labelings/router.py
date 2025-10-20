from fastapi import APIRouter, Depends, Request, Response, Header
from app.utils.jwt import verify_token
from app.utils.project_header import project_header

from app.features.Labelings.service import get_project_labelings, create_labeling
from app.features.Labelings.models import LabelingModel_Input

router = APIRouter()



@router.get("/")
async def get_all_labelings(project = Depends(project_header), user_data=Depends(verify_token)):
    res = await get_project_labelings(user_data, project)
    return res

@router.post("/", status_code=201)
async def post_labeling(data: LabelingModel_Input, project = Depends(project_header), user_data=Depends(verify_token)):
    res = await create_labeling(user_data, project, data)

# @router.put("/{id}")
# async def update_labeling(id, body: Request, project: str = Header(...), user_data=Depends(verify_token)):
#     body = await body.json()
#     res = updateLabeling(project, id, body)

# @router.delete("/{id}")
# async def delete_labeling(id, project: str = Header(...), user_data=Depends(verify_token)):
#     deleteLabeling(project, id)