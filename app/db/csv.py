from pymongo import MongoClient
from bson.objectid import ObjectId
from app.internal.config import MONGO_URI, DATASTORE_DBNAME, CSV_COLLNAME
from pydantic import BaseModel, ValidationError, validator, Field
from typing import Dict, List
from app.utils.helpers import PyObjectId
import datetime

class DBEntry(BaseModel):
    datasets: List[PyObjectId]
    downloadId: str
    project: PyObjectId
    created_at: datetime.datetime

class csvDB:
    def __init__(self) -> None:
        self.mongo_client = MongoClient(MONGO_URI)
        self.db = self.mongo_client[DATASTORE_DBNAME]
        self.col = self.db[CSV_COLLNAME]
        self.col.create_index("created_at", expireAfterSeconds = 10)

    def add(self, datasts, project, download_id):
        data = {"datasets": datasts, "downloadId": download_id, "project": project, "created_at": datetime.datetime.utcnow()}
        data = DBEntry.parse_obj(data).dict()
        self.col.insert_one(data)

    def get(self, download_id):
        data = self.col.find_one({"downloadId": download_id})
        data = DBEntry.parse_obj(data)
        return data