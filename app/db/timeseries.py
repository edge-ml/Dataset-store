from pydantic import BaseModel, Field
import uuid
from pymongo import MongoClient
from bson.objectid import ObjectId
from app.models.db_models.timeseries import TimeSeriesModel

COLLECTION_NAME = "timeseries"

class TimeseriesDBManager():

    def __init__(self) -> None:
        self.mongo_client = MongoClient("mongodb://127.0.0.1:27017")
        self.ts_db = self.mongo_client["ts_db"]
        self.ts_collection = self.ts_db["timeSeries"]

    def addTimeSeries(self, ts):
        ts["_id"] = ObjectId(ts["_id"])
        newDoc = self.ts_collection.insert_one(ts)
        return newDoc

    def updateStartEnd(self, _id, start, end):
        self.ts_collection.update_one({"_id": ObjectId(_id)}, {"$set": {"start": int(start), "end": int(end)}})

    def getTimeSeries(self, datasetId):
        return self.ts_collection.find({"datasetId": ObjectId(datasetId)})

    def gettimeSeriesById(self, _id):
        return self.ts_collection.find_one({"_id": ObjectId(_id)})
    
    def deleteTimeSeries(self, _id):
        return self.ts_collection.delete_one({"_id": ObjectId(_id)})