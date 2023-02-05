from pymongo import MongoClient
from bson.objectid import ObjectId
from app.internal.config import MONGO_URI, DATASTORE_DBNAME, DATASTORE_COLLNAME

class DatasetDBManager:

    def __init__(self) -> None:
        self.mongo_client = MongoClient(MONGO_URI)
        self.ds = self.mongo_client[DATASTORE_DBNAME]
        self.ds_collection = self.ds[DATASTORE_COLLNAME]

    def addDataset(self, dataset):
        return self.ds_collection.insert_one(dataset)

    def deleteDatasetById(self, dataset_id, projectID):
        query = {"_id": ObjectId(dataset_id), "projectId": ObjectId(projectID)}
        data = self.ds_collection.find_one(query)
        self.ds_collection.delete_one(query)
        ts = [x["_id"] for x in data["timeSeries"]]
        return ts

    def getDatasetById(self, dataset_id, project_id):
        return self.ds_collection.find_one({"_id": ObjectId(dataset_id), "projectId": ObjectId(project_id)})

    def getDatasetsInProjet(self, project_id):
        datasets = self.ds_collection.find({"projectId": ObjectId(project_id)})
        return datasets
    
    def updateDataset(self, id, project_id, dataset):
        self.ds_collection.replace_one({"_id": ObjectId(id), "projectId": ObjectId(project_id)}, dataset)

    
    # For modifying dataset-labels

    def updateDatasetLabel(self, project_id, dataset_id, labeling_id, label_Id, newLabel):
        query = {"labelings": {"exist": True}, "_id": ObjectId(dataset_id), "projectId": ObjectId(project_id), "labeling.labelingId": labeling_id}
        update = {"$set": {"labelings.$[].labels.$[label]": newLabel}}
        array_filters = [{"label._id": label_Id}]
        self.ds_collection.update_one(query, update, array_filters=array_filters, upsert=True)