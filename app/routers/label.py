from fastapi import APIRouter, Request, Header, Response
from app.controller.label_controller import createLabel

router = APIRouter()

# Labeling-stuff

@router.post("/{id}/{labelingId}")
async def createDatasetLabel(id, labelingId, body: Request, project: str = Header()):
    body = await body.json()
    createLabel(id, project, labelingId, body)

@router.delete("/")
async def deleteDatasetLabel():
    pass

@router.post("/change/{id}")
async def changeDatasetLabel():
    pass