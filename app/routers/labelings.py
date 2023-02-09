from fastapi import APIRouter, Request, Header, Response
from fastapi.param_functions import Depends
from app.routers.dependencies import validate_user

from app.controller.labelingController import getProjectLabelings, createLabeling, updateLabeling, deleteLabeling

from app.utils.json_encoder import JSONEncoder

import json

router = APIRouter()



# Labeling-stuff

@router.get("/")
def get_project_labelings(project: str = Header(), user_data=Depends(validate_user)):
    res = getProjectLabelings(project)
    return Response(json.dumps(res, cls=JSONEncoder), media_type="application/json")

@router.post("/")
async def create_labeling(body: Request, project: str = Header(), user_data=Depends(validate_user)):
    body = await body.json()
    res = createLabeling(project, body)
    return Response(json.dumps(res, cls=JSONEncoder), media_type="application/json")

@router.put("/{id}")
async def update_labeling(id, body: Request, project: str = Header(), user_data=Depends(validate_user)):
    body = await body.json()
    res = updateLabeling(project, id, body)

@router.delete("/{id}")
async def delete_labeling(id, project: str = Header(), user_data=Depends(validate_user)):
    deleteLabeling(project, id)