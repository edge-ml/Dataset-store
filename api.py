from fastapi import FastAPI, Request
from pydantic import BaseModel
from typing import List
from src.controller import Controller
from src.ApiModels import TimeSeries
from fastapi.middleware.cors import CORSMiddleware
import uvicorn 


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ctrl = Controller()

@app.post("/")
async def saveBatch(body: List[TimeSeries]):
    ctrl.addTimeSeriesBatch(body)

@app.post("/batch")
async def getBatch(body: List[str]):
    return ctrl.getTimeSeriesFullBatch(body)


@app.delete("/{_id}")
async def delete(_id):
    return ctrl.deleteTimeSeries(_id)



@app.post("/save/{_id}")
async def save(_id, body):
    return ctrl.addTimeSeries(body)

@app.get("/tsIds/{datasetId}")
async def getDatasetTSIds(datasetId):
    return ctrl.getDatasetTSIds(datasetId)

@app.get("/{datasetId}")
async def load(datasetId):
    return ctrl.getTimeSeriesFull(datasetId)

@app.get("/part/{datasetId}")
async def loadPart(datasetId, start: int = 0, end: int = 0):
    return ctrl.getTimeSeriesPart(datasetId)

@app.post("append/{_id}")
async def append(_id, body):
    # ctrl.appendTimeSeries(body)

    # store = TSStore(_id)
    # store.loadSeries()
    # store.append(body.time, body.data)
    # return {"message": "Success"}

    pass