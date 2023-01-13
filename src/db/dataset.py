from pydantic import BaseModel, Field
import uuid
from pymongo import MongoClient
from bson.objectid import ObjectId

class DatasetDBManager:

    def __init__(self) -> None:
        self.mongo_client = MongoClient("mongodb://127.0.0.1:27017")
        self.ds = self.mongo_client["dataset_store"]
        self.ds_collection = self.ds["datasets"]

    def addDataset(self, dataset):
        return self.ds_collection.insert_one(dataset)

    def deleteDatasetById(self, dataset_id, projectID):
        query = {"_id": ObjectId(dataset_id), "projectId": ObjectId(projectID)}
        data = self.ds_collection.find_one(query)
        self.ds_collection.delete_one(query)
        ts = [x["_id"] for x in data["timeSeries"]]
        return ts

    def getDatasetById(self, dataset_id, project_id):
        print(project_id)
        return self.ds_collection.find_one({"_id": ObjectId(dataset_id), "projectId": ObjectId(project_id)})

    def getDatasetsInProjet(self, project_id):
        datasets = self.ds_collection.find({"projectId": ObjectId(project_id)})
        return datasets