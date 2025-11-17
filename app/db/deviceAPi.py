from pymongo import MongoClient
from bson.objectid import ObjectId
from internal.config import MONGO_URI, DEVICE_API_COLLNAME, PROJECT_DBNAME
from pydantic import BaseModel, ValidationError, validator, Field
from typing import Dict, List



class DeviceApiManager:

    def __init__(self) -> None:
        self.mongo_client = MongoClient(MONGO_URI)
        self.db = self.mongo_client[PROJECT_DBNAME]
        self.col = self.db[DEVICE_API_COLLNAME]

    def get(self, apiKey):
        access_type = "read"
        res = self.col.find_one({"readApiKey": apiKey})
        if res is None:
            res = self.col.find_one({"writeApiKey": apiKey})
            access_type = "write"
        if res is None:
            return None
        
        return {"userId" : res["userId"], "projectId": res["projectId"], "access_type": access_type}