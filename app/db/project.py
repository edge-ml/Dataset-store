from pymongo import MongoClient
from bson.objectid import ObjectId
from app.internal.config import MONGO_URI, PROJECT_DBNAME, PROJECT_COLLNAME

class ProjectDBManager():
    def __init__(self) -> None:
        self.mongo_client = MongoClient(MONGO_URI)
        self.project_db = self.mongo_client[PROJECT_DBNAME]
        self.project_collection = self.project_db[PROJECT_COLLNAME]

    def get_project(self, project_id: ObjectId):
        project = self.project_collection.find_one({"_id": ObjectId(project_id)})
        return project