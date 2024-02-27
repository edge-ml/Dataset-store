from pymongo import MongoClient
from bson.objectid import ObjectId
from internal.config import MONGO_URI, DATASTORE_DBNAME, DATASTORE_COLLNAME
from pydantic import BaseModel, ValidationError, validator, Field
from typing import Dict, List, Optional, Union
from utils.helpers import PyObjectId
from enum import Enum
import re


# Typing
from models.db import DatasetDBSchema
from models.db.datasets import TimeSeries


class DatasetDBManager:

    def __init__(self) -> None:
        self.mongo_client = MongoClient(MONGO_URI)
        self.db = self.mongo_client[DATASTORE_DBNAME]
        self.ds_collection = self.db[DATASTORE_COLLNAME]

    def addDataset(self, dataset):
        print(dataset)
        dataset = DatasetDBSchema.parse_obj(dataset).dict(by_alias=True)
        self.ds_collection.insert_one(dataset)
        return dataset

    def deleteDatasetById(self, projectID, dataset_id):
        query = {"_id": ObjectId(dataset_id), "projectId": ObjectId(projectID)}
        data = self.ds_collection.find_one(query)
        self.ds_collection.delete_one(query)
        ts = [x["_id"] for x in data["timeSeries"]]
        return ts

    def getDatasetById(self, dataset_id : PyObjectId, project_id : PyObjectId) -> DatasetDBSchema:
        dataset =  self.ds_collection.find_one({"_id": ObjectId(dataset_id), "projectId": ObjectId(project_id)})
        return DatasetDBSchema(**dataset)

    def getDatasetsInProjet(self, project_id) -> List[DatasetDBSchema]:
        datasets = self.ds_collection.find({"projectId": ObjectId(project_id)})
        datasets = [DatasetDBSchema(**x) for x in datasets]
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
    
    def partialUpdate(self, id, project_id, updates: dict):
        self.ds_collection.update_one(
            {"_id": ObjectId(id), "projectId": ObjectId(project_id)},
            {"$set": updates}
        )
    
    def updateTimeSeriesUnit(self, id, timeSeriesId, project_id, unit):
        query = {"_id": ObjectId(id), "projectId": ObjectId(project_id), "timeSeries._id": ObjectId(timeSeriesId)}
        print("unit:", unit)
        update = {"$set": {"timeSeries.$.unit": unit}}
        result = self.ds_collection.update_one(query, update)
        print(result)

    def updateTimeSeriesUnitConfig(self, dataset_id, timeSeries_id, project_id, unit, scaling, offset):
        query = {"_id": ObjectId(dataset_id), "timeSeries._id": ObjectId(timeSeries_id), "projectId": ObjectId(project_id)}
        update = {"$set": {"timeSeries.$.unit": unit, "timeSeries.$.scaling": float(scaling), "timeSeries.$.offset": float(offset)}}
        update_result = self.ds_collection.update_one(query, update)
        
    def deleteProject(self, project):
        self.ds_collection.delete_many({"_id": ObjectId(project)})


    # For modifying dataset-labels

    def updateDatasetLabel(self, project_id, dataset_id, labeling_id, label_Id, newLabel):
        newLabel = DatasetLabel(newLabel)
        query = {"labelings": {"exist": True}, "_id": ObjectId(dataset_id), "projectId": ObjectId(project_id), "labeling.labelingId": labeling_id}
        update = {"$set": {"labelings.$[].labels.$[label]": newLabel}}
        array_filters = [{"label._id": label_Id}]
        self.ds_collection.update_one(query, update, array_filters=array_filters, upsert=True)

    def updateTimeSeriesUnit(self, id, timeSeriesId, project_id, unit):
        query = {"_id": ObjectId(id), "projectId": ObjectId(project_id), "timeSeries._id": ObjectId(timeSeriesId)}
        print("unit:", unit)
        update = {"$set": {"timeSeries.$.unit": unit}}
        result = self.ds_collection.update_one(query, update)
        print(result)