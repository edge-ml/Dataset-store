from pymongo import MongoClient
from bson.objectid import ObjectId
from app.internal.config import MONGO_URI, DATASTORE_DBNAME, CSV_COLLNAME
from pydantic import BaseModel, ValidationError, validator, Field
from typing import Dict, List, Union
from app.utils.helpers import PyObjectId
import datetime


class DBEntryDataset(BaseModel):
    downloadId: str
    projectId: PyObjectId
    userId: PyObjectId
    created_at: datetime.datetime = Field(default_factory=datetime.datetime.now)
    status: int = 0
    error: str = ""
    fileName: str = ""
    filePath : str = ""
    projectName: str
    datasetName: str


class DBEntryProject(BaseModel):
    downloadId: str
    projectId: PyObjectId
    userId: PyObjectId
    created_at: datetime.datetime = Field(default_factory=datetime.datetime.now)
    status: int = 0
    error: str = ""
    projectName: str
    filePath : str = ""

class DBEntry(BaseModel):
    __root__: Union[DBEntryDataset, DBEntryProject]

# DBEntry = Union[DBEntryDataset, DBEntryProject]

print(DBEntry)

class csvDB:
    def __init__(self) -> None:
        self.mongo_client = MongoClient(MONGO_URI)
        self.db = self.mongo_client[DATASTORE_DBNAME]
        self.col = self.db[CSV_COLLNAME]

    # def add(self, download_id, projectId, type, user_id):
    #     data = {"downloadId": download_id, "projectId": projectId, "userId": user_id, "error": "", "type": type, "created_at": datetime.datetime.utcnow()}
    #     data = DBEntry.parse_obj(data).dict()
    #     res = self.col.insert_one(data)

    def add(self, data: DBEntry):
        res = self.col.insert_one(data.dict())
        

    def get(self, download_id):
        data = self.col.find_one({"downloadId": download_id})
        data = DBEntry.parse_obj(data).__root__
        return data
    
    def get_by_user(self, user_id):
        data = self.col.find({"userId": user_id})
        data = [DBEntry.parse_obj(x).__root__ for x in data]
        return data
    
    def update(self, download_id=None, status=None, error=None, fileName=None, filePath=None):
        update_fields = {}
        if status is not None:
            update_fields["status"] = status
        if error is not None:
            update_fields["error"] = error
        if fileName is not None:
            update_fields["fileName"] = fileName
        if filePath is not None:
            update_fields["filePath"] = filePath


        if update_fields:
            self.col.update_one({"downloadId": download_id}, {"$set": update_fields})
    

    def delete(self, download_id):
        self.col.delete_one({"downloadId": download_id})
        
    
    def getOlder(self, time_delta):
        time_threshold = datetime.datetime.utcnow() - datetime.timedelta(seconds=time_delta)
        data = self.col.find({"created_at": {"$lt": time_threshold}})
        return [DBEntry.parse_obj(entry).__root__ for entry in data]