from fastapi import APIRouter, Request, Header, Response
from fastapi.param_functions import Depends

from app.controller.label_controller import createLabel, updateLabel, deleteLabel
from app.utils.json_encoder import JSONEncoder
from app.routers.dependencies import validate_user

import json

router = APIRouter()

# Labeling-stuff

@router.post("/{id}/{labelingId}")
async def createDatasetLabel(id, labelingId, body: Request, project: str = Header(), user_data=Depends(validate_user)):
    body = await body.json()
    createdLabel = createLabel(id, project, labelingId, body)
    return Response(json.dumps(createdLabel, cls=JSONEncoder), media_type="application/json")

@router.put("/{dataset_id}/{labeling_id}/{label_id}")
async def changeDatasetLabel(dataset_id, labeling_id, label_id, body: Request, project: str = Header(), user_data=Depends(validate_user)):
    body = await body.json()
    updateLabel(project, dataset_id, labeling_id, label_id, body)

@router.delete("/{dataset_id}/{labeling_id}/{label_id}")
async def deleteDatasetLabel(dataset_id, labeling_id, label_id, project: str = Header(), user_data=Depends(validate_user)):
    deleteLabel(project, dataset_id, labeling_id, label_id)