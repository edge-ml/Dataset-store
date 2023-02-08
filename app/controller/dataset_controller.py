from app.db.dataset import DatasetDBManager
from .binary_store import BinaryStore
from typing import Union
from bson import ObjectId
import time
import os
from fastapi import HTTPException, status
from app.utils.helpers import custom_index



class DatasetController():

    def __init__(self):

        if not os.path.exists("DATA"):
            os.mkdir("DATA")

        self.dbm = DatasetDBManager()

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



    def addDataset(self, dataset, project):
        datasetMeta = dataset
        datasetMeta["projectId"] = ObjectId(project)
        newDatasetMeta = self.dbm.addDataset(datasetMeta)
        for t, newt in zip(datasetMeta["timeSeries"], newDatasetMeta["timeSeries"]):
            metaData, tsValues = self._splitMeta_Data(t)
            binStore = BinaryStore(newt["_id"])
            binStore.append(tsValues)

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
        ts_ids = self.dbm.deleteDatasetById(id, projectId)
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
    
