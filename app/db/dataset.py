from pymongo import MongoClient
from bson.objectid import ObjectId
from app.internal.config import MONGO_URI, DATASTORE_DBNAME, DATASTORE_COLLNAME
from pydantic import BaseModel, ValidationError, validator, Field
from typing import Dict, List, Optional
from app.utils.helpers import PyObjectId
import re

class SamplingRate(BaseModel):
    mean: float
    var: float

class TimeSeries(BaseModel):
    id: PyObjectId = Field(default_factory=ObjectId, alias="_id")
    start: Optional[int]
    end: Optional[int]
    unit: str = Field(default="")
    name: str
    samplingRate: Optional[SamplingRate]
    length: Optional[int]


class DatasetLabel(BaseModel):
    start: int
    end: int
    type: PyObjectId = Field(default_factory=ObjectId)
    id: PyObjectId = Field(default_factory=ObjectId, alias="_id")
    metaData: Dict[str, str] = Field(default={})

class DatasetLabeling(BaseModel):
    labelingId: PyObjectId = Field(default_factory=ObjectId)
    labels: List[DatasetLabel]

class DatasetSchema(BaseModel):
    id: PyObjectId = Field(default_factory=ObjectId, alias="_id")
    projectId: PyObjectId
    name: str
    metaData: Dict[str, str] = Field(default={})
    timeSeries: List[TimeSeries]
    labelings: List[DatasetLabeling] = Field(default=[])
    userId: PyObjectId


class DatasetDBManager:

    def __init__(self) -> None:
        self.mongo_client = MongoClient(MONGO_URI)
        self.db = self.mongo_client[DATASTORE_DBNAME]
        self.ds_collection = self.db[DATASTORE_COLLNAME]

    def addDataset(self, dataset):
        dataset = DatasetSchema.parse_obj(dataset).dict(by_alias=True)
        self.ds_collection.insert_one(dataset)
        return dataset

    def deleteDatasetById(self, projectID, dataset_id):
        query = {"_id": ObjectId(dataset_id), "projectId": ObjectId(projectID)}
        data = self.ds_collection.find_one(query)
        self.ds_collection.delete_one(query)
        ts = [x["_id"] for x in data["timeSeries"]]
        return ts

    def getDatasetById(self, dataset_id, project_id):
        return self.ds_collection.find_one({"_id": ObjectId(dataset_id), "projectId": ObjectId(project_id)})

    def getDatasetsInProjet(self, project_id):
        datasets = self.ds_collection.find({"projectId": ObjectId(project_id)})
        return datasets
    
    def getDatasetsInProjectByPage(self, project_id, page, page_size, sort, filters):
        # Calculate the number of datasets to skip to reach the desired page
        skip_count = (page - 1) * page_size
        query = {"projectId": ObjectId(project_id)}
        pipeline = []
        #filter for labelings and labels
        if filters and 'labelings' in filters:
            pipeline.append({
                "$match": {
                    "$and": [
                        query,
                        {
                            "$or": [
                                {"labelings": {"$elemMatch": {"labelingId": {"$in":  [ObjectId(id_str) for id_str in filters['labelings']['target_labeling_ids']]}}}},
                                {"labelings.labels": {"$elemMatch": {"type": {"$in": [ObjectId(id_str) for id_str in filters['labelings']['target_label_ids']]}}}}
                            ]          
                        }
                    ]
                }
            })
        elif filters and 'filterEmptyDatasets' in filters:
            pipeline.append({
                "$match": {
                    "$and": [
                        query,
                        {
                            "$expr": {
                                "$allElementsTrue": {
                                    "$map": {
                                        "input": "$timeSeries",
                                        "as": "elm",
                                        "in": {
                                            "$or": [
                                                {"$eq": ["$$elm.length", 0]},
                                                {"$eq": ["$$elm.length", None]},
                                            ]
                                        },
                                    }
                                }
                            }
                        },
                    ]
                }
            })
        elif filters and 'filterByName' in filters:
            if(filters['filterByName']):
                search_string = re.escape(filters['filterByName'])
                regex_pattern = f".*{search_string}.*"
                pipeline.append({
                    "$match": {
                        "$and": [
                         query,
                         {"name": {"$regex": regex_pattern, "$options": "i"}}
                         ]
                        }
                })
            else:
                pipeline.append({"$match": query})
        #no filters applied
        else: 
            pipeline.append({"$match": query})

        #count ds at this stage
        pipeline.append( {"$facet": {
        "datasets": [],
        "count": [
            {"$count": "count"}
        ]
    }})

        #sorting
        if(sort == 'alphaAsc' or sort == 'alphaDesc'):
            sortField = 'name'
            sortOrder = 0

            if sort == 'alphaAsc':
                sortOrder = 1
            elif sort == 'alphaDesc':
                sortOrder = -1
            pipeline[1]['$facet']['datasets'].extend([{"$sort": {sortField: sortOrder}},
            #{"$collation": {'locale': 'en', 'strength': 2}}
            ])
        
        if(sort == 'dateAsc' or sort == 'dateDesc'):
            sortOrder = 0
            if(sort == 'dateAsc'):
                sortOrder = 1
            else:
                sortOrder = -1   
            pipeline[1]['$facet']['datasets'].extend([{"$addFields": {"min_start": {"$min": "$timeSeries.start"}}},
                {"$sort": {"min_start": sortOrder}}])
        
        #add pagination to pipeline 
        pipeline[1]['$facet']['datasets'].extend([{"$skip": skip_count},
                {"$limit": page_size}])
        
        #ds count and datasets
        result = list(self.ds_collection.aggregate(pipeline))
        datasets = result[0]["datasets"]
        total_count = 0
        if result and result[0].get("count"):
            total_count = result[0]["count"][0]["count"]
        return datasets, total_count

    def updateDataset(self, id, project_id, dataset):
        dataset = DatasetSchema.parse_obj(dataset).dict(by_alias=True)
        self.ds_collection.replace_one({"_id": ObjectId(id), "projectId": ObjectId(project_id)}, dataset)
        return dataset

    def deleteProject(self, project):
        self.ds_collection.delete_many({"_id": ObjectId(project)})


    # For modifying dataset-labels

    def updateDatasetLabel(self, project_id, dataset_id, labeling_id, label_Id, newLabel):
        newLabel = DatasetLabel(newLabel)
        query = {"labelings": {"exist": True}, "_id": ObjectId(dataset_id), "projectId": ObjectId(project_id), "labeling.labelingId": labeling_id}
        update = {"$set": {"labelings.$[].labels.$[label]": newLabel}}
        array_filters = [{"label._id": label_Id}]
        self.ds_collection.update_one(query, update, array_filters=array_filters, upsert=True)