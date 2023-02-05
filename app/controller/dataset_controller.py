from app.db.dataset import DatasetDBManager
from .binary_store import BinaryStore
from typing import Union
from bson import ObjectId
import time
import os



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
        if "metaData" not in datasetMeta:
            datasetMeta["metaData"] = {}

        for i, t in enumerate(datasetMeta["timeSeries"]):
            metaData, tsValues = self._splitMeta_Data(t)
            objectId = ObjectId()
            binStore = BinaryStore(objectId)
            binStore.append(tsValues)
            datasetMeta["timeSeries"][i]["_id"] = ObjectId(objectId)

        newDatasetMeta = self.dbm.addDataset(datasetMeta)

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
        t_start = time.time()
        for t in ts_ids:
            binStore = BinaryStore(t)
            binStore.loadSeries()
            d = binStore.getPart(start, end, max_resolution)
            res.append(d)
        return res