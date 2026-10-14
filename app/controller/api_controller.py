from db.dataset import DatasetDBManager
from controller.binary_store import BinaryStore
from controller.dataset_controller import DatasetController
from controller.labelingController import createLabeling
from controller.label_controller import createLabel
from utils.helpers import random_hex_color
from bson.objectid import ObjectId
from bson.errors import InvalidId
from fastapi import HTTPException
import numpy as np

dbm = DatasetDBManager()
ctrl = DatasetController()

def initDataset(name, timeSeries, metaData, userId, projectId):
    newTimeSeries = [{"name": x, "start": 2**63-1, "end": -1} for x in timeSeries]
    
    newDataset = {"name": name, "start": 2**63-1, "end": -1, "timeSeries": newTimeSeries, "userId": userId, "projectId": projectId, "metaData": metaData}
    newDataset = dbm.addDataset(newDataset)
    for ts in newDataset["timeSeries"]:
        binStore = BinaryStore(ts["_id"])
        binStore.saveSeries()
    return str(newDataset["_id"])

def appendDataset(uploadData, userId, projectId, dataset_id):
    try:
        dataset = dbm.getDatasetById(dataset_id=dataset_id, project_id=projectId)
    except InvalidId:
        dataset = None
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    name_to_id = {x["name"]: x["_id"] for x in dataset["timeSeries"]}
    unknown = [ts.name for ts in uploadData.data if ts.name not in name_to_id]
    if unknown:
        raise HTTPException(status_code=400, detail=f"Unknown time-series {', '.join(unknown)}. "
                                                    f"The dataset has: {', '.join(name_to_id)}")
    for label in uploadData.labels:
        if label.end < label.start:
            raise HTTPException(status_code=400, detail=f"Label '{label.labelName}' ends before it starts")

    for ts in uploadData.data:
        id = name_to_id[ts.name]
        binStore = BinaryStore(id)
        binStore.loadSeries()
        start, end, sampling_rate, length = binStore.append(np.array(ts.data))
        index = next((i for i, item in enumerate(dataset["timeSeries"]) if item["_id"] == id), -1)
        dataset["timeSeries"][index]["length"] = length
        dataset["timeSeries"][index]["samplingRate"] = sampling_rate
        dataset["timeSeries"][index]["start"] = start
        dataset["timeSeries"][index]["end"] = end
    if uploadData.data:
        dbm.updateDataset(dataset["_id"], projectId, dataset)

    labels = [(l.labelingName, l.labelName, l.start, l.end) for l in uploadData.labels]
    if uploadData.labeling:
        tmpStart = min(x["start"] for x in dataset["timeSeries"])
        tmpEnd = max(x["end"] for x in dataset["timeSeries"])
        labels.append((uploadData.labeling.labelingName, uploadData.labeling.labelName, tmpStart, tmpEnd))
    for labelingName, labelName, start, end in labels:
        # createLabeling merges into an existing labeling of the same name
        createdLabeling = createLabeling(projectId, {"name": labelingName, "labels": [{"name": labelName, "color": random_hex_color()}]})
        labelType = [x for x in createdLabeling["labels"] if x["name"] == labelName][0]["_id"]
        createLabel(dataset_id, projectId, createdLabeling["_id"], {"start": start, "end": end, "type": ObjectId(labelType)})
