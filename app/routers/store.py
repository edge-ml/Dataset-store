from fastapi import APIRouter
from typing import List
from app.controller import Controller
from app.models.api_models.timeseries import TimeSeries

router = APIRouter()
ctrl = Controller()

@router.post("/")
async def saveBatch(body: List[TimeSeries]):
    ctrl.addTimeSeriesBatch(body)

@router.post("/batch")
async def getBatch(body: List[str]):
    return ctrl.getTimeSeriesFullBatch(body)

@router.delete("/{_id}")
async def delete(_id):
    return ctrl.deleteTimeSeries(_id)

@router.post("/save/{_id}")
async def save(_id, body):
    return ctrl.addTimeSeries(body)

@router.get("/tsIds/{datasetId}")
async def getDatasetTSIds(datasetId):
    return ctrl.getDatasetTSIds(datasetId)

@router.get("/{datasetId}")
async def load(datasetId):
    return ctrl.getTimeSeriesFull(datasetId)

@router.get("/part/{datasetId}")
async def loadPart(datasetId, start: int = 0, end: int = 0):
    return ctrl.getTimeSeriesPart(datasetId)

@router.post("append/{_id}")
async def append(_id, body):
    # ctrl.appendTimeSeries(body)

    # store = TSStore(_id)
    # store.loadSeries()
    # store.append(body.time, body.data)
    # return {"message": "Success"}

    pass