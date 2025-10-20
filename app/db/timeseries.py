from pymongo import MongoClient
from bson.objectid import ObjectId
from app.internal.config import MONGO_URI, TIMESERIES_DBNAME, TIMESERIES_COLLNAME

class TimeseriesDBManager():

    def __init__(self) -> None:
        self.mongo_client = MongoClient(MONGO_URI)
        self.ts_db = self.mongo_client[TIMESERIES_DBNAME]
        self.ts_collection = self.ts_db[TIMESERIES_COLLNAME]

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