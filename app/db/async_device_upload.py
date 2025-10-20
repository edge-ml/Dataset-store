import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId
from app.internal.config import MONGO_URI, DATASTORE_DBNAME, ASYNC_UPLOAD_COLNAME
from pydantic import BaseModel, Field
from typing import Dict, List, Union
from app.utils.helpers import PyObjectId
import datetime

class UploadRequest(BaseModel):
    id: str = Field(alias="_id")
    status: int = Field(default=0)
    created_at: datetime.datetime = Field(default_factory=datetime.datetime.now)
    error: str = ""
    user_id: PyObjectId

class AsyncUploadDB:
    def __init__(self) -> None:
        self.mongo_client = MongoClient(MONGO_URI)
        self.db = self.mongo_client[DATASTORE_DBNAME]
        self.col = self.db[ASYNC_UPLOAD_COLNAME] 
        
        self.col.create_index("created_at", expireAfterSeconds=60*60*24*365)

    def add_upload_request(self, req: UploadRequest):
        res = self.col.insert_one(req.dict(by_alias=True))

    def setStatus_finished(self, id):
        self.col.update_one({"_id": id}, {"$set": {"status": 100}})

    def setError(self, id, error):
        print("Setting error")
        self.col.update_one({"_id": id}, {"$set": {"error": error}})

    def getStatus(self, id, user_id):
        return UploadRequest.parse_obj(self.col.find_one({"_id": id, "user_id": user_id}))
