from pymongo import MongoClient
from bson.objectid import ObjectId
from app.internal.config import MONGO_URI, PROJECT_COLLNAME, PROJECT_DBNAME, LABELING_COLLNAME
from pydantic import BaseModel, ValidationError, validator, Field
from typing import Dict, List
from app.utils.helpers import PyObjectId


class LabelModel(BaseModel):
    name: str
    color: str
    id: PyObjectId = Field(default_factory=ObjectId, alias="_id")

class LabelingModel(BaseModel):
    name: str
    labels: List[LabelModel]
    projectId: PyObjectId
    id: PyObjectId = Field(alias="_id")



class LabelingDBManager:

    def __init__(self) -> None:
        self.mongo_client = MongoClient(MONGO_URI)
        self.db = self.mongo_client[PROJECT_DBNAME]
        self.col = self.db[LABELING_COLLNAME]

        # Create unique index for the labeling name
        self.col.create_index([("name", 1), ("projectId", 1)], unique=True)

    def get(self, projectId):
        res = self.col.find({"projectId": ObjectId(projectId)})
        res = list(res)
        return res if res is not None else []

    def get_single(self, projectId, labeling_id):
        res = self.col.find_one({"projectId": ObjectId(projectId), "_id": ObjectId(labeling_id)})
        return res

    def create(self, projectId, labeling):
        labeling = LabelingModel.parse_obj(labeling).dict(by_alias=True)
        query = {"projectId": ObjectId(projectId), "name": labeling["name"]}
        currentLabeling = self.col.find_one(query)
        if currentLabeling is None: #Labeling with this name not present in project
            self.col.insert_one(labeling)
            return labeling
        else: # Labeling already present
            currentLabelNames = [x["name"] for x in currentLabeling["labels"]]
            for label in labeling["labels"]:
                if label["name"] not in currentLabelNames:
                    currentLabeling["labels"].append(label)
            query = {"projectId": ObjectId(projectId), "_id": ObjectId(currentLabeling["_id"])}
            self.col.replace_one(query, currentLabeling)
            return currentLabeling
            

    def update(self, project_id, lableing_id, labeling):
        query = {"projectId": ObjectId(project_id), "_id": ObjectId(lableing_id)}
        labeling = LabelingModel.parse_obj(labeling).dict(by_alias=True)
        # oldLabeling = self.col.find_one(query)
        # oldLabelIds = set([ObjectId(x["_id"]) for x in oldLabeling["labels"]])
        # newLabelIds = set([ObjectId(x["_id"]) for x in labeling["labels"]])
        self.col.replace_one(query, labeling)
        return labeling
    
    def delete(self, project, id):
        self.col.delete_one({"projectId": ObjectId(project), "_id": ObjectId(id)})

    def deleteProject(self, project):
        self.col.delete_many({"project_id": ObjectId(project)})