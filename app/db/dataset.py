from pymongo import MongoClient
from bson.objectid import ObjectId
from internal.config import MONGO_URI, DATASTORE_DBNAME, DATASTORE_COLLNAME
from pydantic import BaseModel, ValidationError, validator, Field
from typing import Dict, List, Optional, Union
from utils.helpers import PyObjectId
from enum import Enum
import re

class ProgressStep(Enum):
    PARSING = ["Parsing the file", 20]
    LABELING = ["Extracting labels", 40]
    CREATING_DATASET = ["Creating dataset", 60]
    UPLOADING_DATASET = ["Syncing Timeseries with DB", 80]
    COMPLETE = ["Complete", 100]
    
class SamplingRate(BaseModel):
    mean: float
    var: float

class TimeSeries(BaseModel):
    id: PyObjectId = Field(default_factory=ObjectId, alias="_id")
    start: int | None = None
    end: int | None = None
    unit: str = Field(default="")
    # Physical quantity: a catalog id (see ml /units), a custom label, or None
    # when unknown (every series uploaded before quantities existed).
    quantity: Optional[str] = None
    name: str
    samplingRate: SamplingRate | None = None
    length: int | None = None


class MediaSeries(BaseModel):
    """An audio or video recording placed on the dataset timeline.

    Kept apart from timeSeries on purpose: everything that reads timeSeries
    (training, CSV export, the device libraries) expects numeric samples.
    """
    id: PyObjectId = Field(default_factory=ObjectId, alias="_id")
    kind: str  # "audio" | "video"
    name: str
    start: int  # unix ms at which the recording begins
    end: int  # start + durationMs
    durationMs: int
    mimeType: str
    size: int  # bytes
    status: str = "ready"  # "processing" while the waveform is computed
    hasPeaks: bool = False
    meta: Dict[str, Union[str, int, float, None]] = Field(default={})


class DatasetLabel(BaseModel):
    start: int
    end: int
    type: PyObjectId = Field(default_factory=ObjectId)
    id: PyObjectId = Field(default_factory=ObjectId, alias="_id")
    metaData: Dict[str, str] = Field(default={})

    @validator('end')
    def check_start_end(cls, v, values):
        # Allow zero-duration (point) labels, i.e. a label marked on a single
        # timestamp; only reject end strictly before start.
        if 'start' in values and v < values['start']:
            raise ValueError('end must not be before start')
        return v

class DatasetLabeling(BaseModel):
    labelingId: PyObjectId = Field(default_factory=ObjectId)
    labels: List[DatasetLabel]

class DatasetSchema(BaseModel):
    id: PyObjectId = Field(default_factory=ObjectId, alias="_id")
    projectId: PyObjectId
    name: str
    metaData: Dict[str, str] = Field(default={})
    timeSeries: List[TimeSeries] = Field(default=[])
    labelings: List[DatasetLabeling] = Field(default=[])
    mediaSeries: List[MediaSeries] = Field(default=[])
    userId: PyObjectId

class DatasetDBManager:

    def __init__(self) -> None:
        self.mongo_client = MongoClient(MONGO_URI)
        self.db = self.mongo_client[DATASTORE_DBNAME]
        self.ds_collection = self.db[DATASTORE_COLLNAME]

    def addDataset(self, dataset):
        print(dataset)
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
    
    def getDatasetsInProjetPagination(self, project_id, skip, limit, sort, filters):

        sort_map = {
            "alphaDesc": {"field": "name", "order": -1},
            "alphaAsc": {"field": "name", "order": 1},
            "dateDesc": {"field": "timeSeries.start", "order": -1},
            "dateAsc": {"field": "timeSeries.start", "order": 1}
        }

        query = {"projectId": ObjectId(project_id)}

        sortingOptions = {
            "alphaAsc": ("name", 1),
            "alphaDesc": ("name", -1),
            "dateAsc": ("timeSeries.start", 1),
            "dateDesc": ("timeSeries.start", -1)
        }

        sorting_field, sorting_direction = sortingOptions[sort]
        datasets = self.ds_collection.find(query).skip(skip).limit(limit).sort(sorting_field, sorting_direction)
        dataset_count = self.ds_collection.count_documents(query)
        return datasets, dataset_count

    def updateDataset(self, id, project_id, dataset):
        dataset = DatasetSchema.parse_obj(dataset).dict(by_alias=True)
        # Recordings are only changed through the media methods below. Callers
        # here send whole datasets that may predate an upload (or come from a
        # client that does not know about recordings) and must not drop them.
        fields = {k: v for k, v in dataset.items() if k not in ("_id", "mediaSeries")}
        self.ds_collection.update_one({"_id": ObjectId(id), "projectId": ObjectId(project_id)}, {"$set": fields})
        return dataset

    # Audio and video recordings

    def addMedia(self, dataset_id, project_id, media):
        media = MediaSeries.parse_obj(media).dict(by_alias=True)
        query = {"_id": ObjectId(dataset_id), "projectId": ObjectId(project_id)}
        if self.ds_collection.update_one(query, {"$push": {"mediaSeries": media}}).matched_count != 1:
            return None
        return media

    def getMedia(self, dataset_id, project_id, media_id):
        dataset = self.getDatasetById(dataset_id, project_id)
        if dataset is None:
            return None
        return next((m for m in dataset.get("mediaSeries", []) if str(m["_id"]) == str(media_id)), None)

    def updateMedia(self, dataset_id, project_id, media_id, updates: dict):
        query = {"_id": ObjectId(dataset_id), "projectId": ObjectId(project_id), "mediaSeries._id": ObjectId(media_id)}
        update = {"$set": {f"mediaSeries.$.{k}": v for k, v in updates.items()}}
        return self.ds_collection.update_one(query, update).matched_count == 1

    def deleteMedia(self, dataset_id, project_id, media_id):
        query = {"_id": ObjectId(dataset_id), "projectId": ObjectId(project_id), "mediaSeries._id": ObjectId(media_id)}
        update = {"$pull": {"mediaSeries": {"_id": ObjectId(media_id)}}}
        return self.ds_collection.update_one(query, update).matched_count == 1
    
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

    def setTimeSeriesQuantityUnit(self, dataset_id, timeSeries_id, project_id, quantity, unit):
        """Relabel what a series measures. The stored values are not touched."""
        query = {"_id": ObjectId(dataset_id), "timeSeries._id": ObjectId(timeSeries_id), "projectId": ObjectId(project_id)}
        update = {"$set": {"timeSeries.$.quantity": quantity, "timeSeries.$.unit": unit}}
        return self.ds_collection.update_one(query, update).matched_count == 1

    def updateTimeSeriesUnitConfig(self, dataset_id, timeSeries_id, project_id, unit, scaling, offset):
        query = {"_id": ObjectId(dataset_id), "timeSeries._id": ObjectId(timeSeries_id), "projectId": ObjectId(project_id)}
        update = {"$set": {"timeSeries.$.unit": unit, "timeSeries.$.scaling": float(scaling), "timeSeries.$.offset": float(offset)}}
        update_result = self.ds_collection.update_one(query, update)
        return update_result
    def deleteProject(self, project):
        self.ds_collection.delete_many({"projectId": ObjectId(project)})


    # For modifying dataset-labels

    def updateDatasetLabel(self, project_id, dataset_id, labeling_id, label_Id, newLabel):
        newLabel = DatasetLabel.parse_obj(newLabel).dict(by_alias=True)
        dataset = self.getDatasetById(dataset_id, project_id)
        if dataset is None:
            return None
        for labeling in dataset["labelings"]:
            if ObjectId(labeling["labelingId"]) != ObjectId(labeling_id):
                continue
            for i, label in enumerate(labeling["labels"]):
                if str(label["_id"]) == str(label_Id):
                    labeling["labels"][i] = newLabel
        self.ds_collection.replace_one(
            {"_id": ObjectId(dataset_id), "projectId": ObjectId(project_id)}, dataset)
        return newLabel