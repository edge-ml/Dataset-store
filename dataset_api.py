from fastapi import FastAPI, Request, Header, Response
from pydantic import BaseModel
from typing import List
from src.ApiModels import TimeSeries, Dataset
from fastapi.middleware.cors import CORSMiddleware
import uvicorn 
import json
from src.Utils.JsonEncoder import JSONEncoder
import time
import orjson

from src.controller.datasetController import DatasetController

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ctrl = DatasetController()

# Create datset
@app.post("/")
async def createDataset(body: Request, project: str = Header()):
    body = await body.json()
    ctrl.addDataset(dataset=body, project=project)
    return {"message": "success"}


@app.get("/project")
async def getDatasetsInProject(project: str = Header()):
    data = ctrl.getDatasetInProject(project)
    return Response(json.dumps(data, cls=JSONEncoder), media_type="application/json")

# Get dataset
@app.get("/{id}")
async def getDataset(id, project: str = Header()):
    dataset = ctrl.getDatasetById(id, project)
    return Response(json.dumps(dataset, cls=JSONEncoder), media_type="application/json")


@app.get("/{id}/ts/{start}/{end}/{max_resolution}")
async def getTimeSeriesDataset(id, start, end, max_resolution, project: str = Header()):
    t = time.time()
    dataset = ctrl.getDataSetByIdStartEnd(id, project, start, end, max_resolution)
    t_end = time.time()
    print("generate: ", t_end - t)
    t = time.time()
    # res = json.dumps(dataset, cls=JSONEncoder)
    res = orjson.dumps(dataset, option = orjson.OPT_SERIALIZE_NUMPY)
    t_end = time.time()
    print("Json: ", t_end - t)

    return Response(res, media_type="application/json")

# Get metadata of dataset
@app.get("/{id}/meta")
async def getDatasetMetaData(id, project : str = Header()):
    dataset = ctrl.getDatasetById(id, project, onlyMeta=True)
    return Response(json.dumps(dataset, cls=JSONEncoder), media_type="application/json")

# Delete dataset
@app.delete("/{id}")
async def deleteDatasetById(id, project: str = Header()):
    ctrl.deleteDataset(id, projectId=project)
