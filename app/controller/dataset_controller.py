from app.db.dataset import DatasetDBManager
from .binary_store import BinaryStore
from typing import Union
from bson import ObjectId
import time
import os
from fastapi import HTTPException, status
from app.utils.helpers import custom_index
from app.db.deviceAPi import DeviceApiManager
import requests
import random
from app.internal.config import BACKEND_URI
from app.controller.labelingController import createLabeling


class DatasetController():

    def __init__(self):

        if not os.path.exists("DATA"):
            os.mkdir("DATA")

        self.dbm = DatasetDBManager()
        self.deviceAPI = DeviceApiManager()
    def _splitMeta_Data(self, timeSeries):
        tsValues = timeSeries["data"]
        metaData = timeSeries
        del metaData["data"]
        return metaData, tsValues

    def _convertObejctIdsToStr(self, data):
        data["projectId"] = str(data["projectId"])
        data["_id"] = str(data["_id"])
        for i, t in enumerate(data["timeSeries"]):
            data["timeSeries"][i]["_id"] = str(t["_id"])
        return data

    def getDatasetById(self, dataset_id, project, onlyMeta=False):
        # Read dataset from database
        datasetMeta = self.dbm.getDatasetById(dataset_id, project)
        if onlyMeta:
            return datasetMeta
        for t in datasetMeta["timeSeries"]:
            binStore = BinaryStore(t["_id"])
            binStore.loadSeries()
            data = binStore.getFull()
            t["data"] = [[x, y] for x, y in zip(data["time"].tolist(), data["data"].tolist())]
        return datasetMeta



    def addDataset(self, dataset, project, user_id=None):
        datasetMeta = dataset
        datasetMeta["projectId"] = ObjectId(project)
        if user_id is not None:
            datasetMeta["userId"] = ObjectId(user_id)
        newDatasetMeta = self.dbm.addDataset(datasetMeta)
        try:
            for t, newt in zip(datasetMeta["timeSeries"], newDatasetMeta["timeSeries"]):
                metaData, tsValues = self._splitMeta_Data(t)
                binStore = BinaryStore(newt["_id"])
                binStore.append(tsValues)
        except:
            print("Handle here", newDatasetMeta["_id"])
            self.dbm.deleteDatasetById(project, newDatasetMeta["_id"])
            raise TypeError()
        return newDatasetMeta

    def _convertTimeSeriesObjectIdToStr(self, ts_array):
        res = []
        for t in ts_array:
            t["_id"] = str(t["_id"])
            res.append(t)
        return res

    def getDatasetInProject(self, projectId):
        datasets = self.dbm.getDatasetsInProjet(projectId)
        return list(datasets)

    def deleteDataset(self, id, projectId):
        ts_ids = self.dbm.deleteDatasetById(projectId, id)
        for id in ts_ids:
            binStore = BinaryStore(id)
            binStore.delete()
        
    def getDataSetByIdStartEnd(self, id, projectId, start, end, max_resolution):
        dataset = self.dbm.getDatasetById(id, project_id=projectId)
        ts_ids = [x["_id"] for x in dataset["timeSeries"]]
        res = []
        for t in ts_ids:
            binStore = BinaryStore(t)
            binStore.loadSeries()
            d = binStore.getPart(start, end, max_resolution)
            res.append(d)
        return res

    def append(self, id, project, body, projectId):
        dataset = self.dbm.getDatasetById(id, project)
        datasetIds = [x["_id"] for x in dataset["timeSeries"]]
        sendIds = [ObjectId(x["id"]) for x in body]
        if set(datasetIds) != set(sendIds):
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Authentication failed")


        newStart = dataset["start"]
        newEnd = dataset["end"]
        for ts in body:
            binStore = BinaryStore(ts["id"])
            binStore.loadSeries()
            tmpStart, tmpEnd = binStore.append(ts["data"])
            newStart = min(newStart, tmpStart)
            newEnd = max(newEnd, tmpEnd)
            binStore.saveSeries()

            idx = custom_index(dataset["timeSeries"], lambda x: ObjectId(x["_id"]) == ObjectId(ts["id"]))
            oldStart = int(dataset["timeSeries"][idx]["start"])
            oldEnd = int(dataset["timeSeries"][idx]["end"])
            dataset["timeSeries"][idx]["start"] = min(oldStart, tmpStart) if oldStart is not None else tmpStart
            dataset["timeSeries"][idx]["end"] = max(oldEnd, tmpEnd) if oldEnd is not None else tmpEnd

        dataset["start"] = int(newStart)
        dataset["end"] = int(newEnd)
        self.dbm.updateDataset(id, project, dataset=dataset)
        return 


    def externalUpload(self, api_key, user_id, body):
        # Get labels from dataset
        dataset = body
        datasetLabels = body["labeling"]["labels"]
        labelingName = body["labeling"]["name"]
        uniquelabels = list(set([x["name"] for x in datasetLabels]))

        # Check if api_key has access rights
        deviceApi = self.deviceAPI.get(api_key)
        project_id = deviceApi["projectId"]
        dataset["userId"] = deviceApi["userId"]

        color_dict = {x: "#%06x" % random.randint(0, 0xFFFFFF) for x in uniquelabels}

        for l in datasetLabels:
            l["color"] = color_dict[l["name"]]
        print(datasetLabels)
        labeling = createLabeling(project_id, {"name": labelingName, "labels": datasetLabels})
        print(labeling)
        typeDict = {x["name"]: x["_id"] for x in labeling["labels"]}
        for l in datasetLabels:
            l["type"] = typeDict[l["name"]]
        dataset["labelings"] = [{"labelingId": labeling["_id"], "labels": datasetLabels}]
        print(dataset)
        metadataset = self.addDataset(dataset=dataset, project=project_id, user_id=user_id)
        return metadataset["_id"]

    def deleteProjectDatasets(self, project):
        datasets = self.dbm.getDatasetsInProjet(project)

        for dataset in datasets:
            timeSeriesIDs = [x["_id"] for x in dataset["timeSeries"]]

            for d in timeSeriesIDs:
                binStore = BinaryStore(d)
                binStore.delete()
        self.dbm.deleteProject(project)