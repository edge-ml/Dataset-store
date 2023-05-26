from pydantic import BaseModel, Field
from pymongo import MongoClient
from bson.objectid import ObjectId
from app.internal.config import MONGO_URI, DATASTORE_DBNAME, ASYNC_UPLOAD_COLNAME
from pydantic import BaseModel, ValidationError, validator, Field
from typing import Dict, List, Union
from app.utils.helpers import PyObjectId
import datetime



class UploadRequest(BaseModel):
    id: str = Field(alias="_id")
    status: int = Field(default=0)
    created_at: datetime.datetime = Field(default_factory=datetime.datetime.now)
    error: str = ""

class AsyncUploadDB:
    def __init__(self) -> None:
        self.mongo_client = MongoClient(MONGO_URI)
        self.db = self.mongo_client[DATASTORE_DBNAME]
        self.col = self.db[ASYNC_UPLOAD_COLNAME]

    def add_upload_request(self, req: UploadRequest):
        res = self.col.insert_one(req.dict(by_alias=True))

    def setStatus_finished(self, id):
        self.col.update_one({"_id": id}, {"$set": {"status": 100}})

    def setError(self, id, error):
        self.col.update_one({"_id": id}, {"$set": {"error": error}})

    def getStatus(self, id):
        return self.col.find_one({"_id": id})