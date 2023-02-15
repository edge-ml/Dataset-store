from pymongo import MongoClient
from bson.objectid import ObjectId
from app.internal.config import MONGO_URI, DEVICE_API_COLLNAME, PROJECT_DBNAME
from pydantic import BaseModel, ValidationError, validator, Field
from typing import Dict, List



class DeviceApiManager:

    def __init__(self) -> None:
        self.mongo_client = MongoClient(MONGO_URI)
        self.db = self.mongo_client[PROJECT_DBNAME]
        self.col = self.db[DEVICE_API_COLLNAME]

    def get(self, apiKey):
        res = self.col.find_one({"deviceApiKey": apiKey})
        return {"userId" : res["userId"], "projectId": res["projectId"]}