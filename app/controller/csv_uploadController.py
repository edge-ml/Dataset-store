from io import StringIO
from app.db.dataset import DatasetDBManager
from .binary_store import BinaryStore
from typing import Union
from bson.objectid import ObjectId
import time
import os
from fastapi import HTTPException, status
from app.utils.helpers import custom_index
from app.db.deviceAPi import DeviceApiManager
import requests
import random
from app.controller.labelingController import createLabeling
from fastapi import UploadFile
from app.utils.CsvParser import CsvParser
import traceback
from pydantic import BaseModel, Field
from typing import Dict, List, Optional
import json
import pandas as pd
from app.db.labelings import LabelingDBManager
from io import BytesIO
from app.db.async_device_upload import AsyncUploadDB, UploadRequest


asyncDB = AsyncUploadDB()
dbm = DatasetDBManager()


class CSVLabel(BaseModel):
    start: str
    end: str
    name: str
    metaData: Optional[Dict[str, str]] = Field(default={})

class CsvLabeling(BaseModel):
    name: str
    labels: List[CSVLabel]

class FileDescriptor(BaseModel):
    name: str
    size: int
    drop: List[str]
    time: List[str]

class CSVDatasetInfo(BaseModel):
    name: str
    files: List[FileDescriptor]
    labeling: Optional[CsvLabeling]
    metaData: Optional[Dict[str, str]]

def generateLabeling(projectId, labeling : CsvLabeling):
    unique_labels_names = [x.name for x in labeling.labels]
    unique_labels = [{"name": x, "color": f'#{"%06x" % random.randint(0, 0xFFFFFF)}'} for x in unique_labels_names]
    return createLabeling(projectId, {"name": labeling.name, "labels": unique_labels})

async def _processData(info, files, projectId, userId, processId):
    try:
        info = CSVDatasetInfo.parse_obj(info)
        dataset_name = info.name
        labeling = info.labeling
        file_info = info.files

        # Add new labeling to the db
        if labeling:
            print("using labels")
            newLabeling = generateLabeling(projectId=projectId, labeling=labeling)

            label_type_map = {x["name"]: x["_id"] for x in newLabeling["labels"]}


            # Assign the type to each dataset-label
            dataset_labels = labeling.dict(by_alias=True)["labels"]
            for x in dataset_labels:
                x["type"] = label_type_map[str(x["name"])]

        # Process each csv-file in the dataset
        start_idx = 0
        tsIds = []
        starts = []
        ends = []
        sampling_rates =[]
        lengths = []
        headers = []
        file_names = []


        try:
            for i, f_info in enumerate(file_info):
                bin = await files[i].read()
                start_idx += f_info.size
                file = CsvParser(bin, drop=f_info.drop, time=f_info.time)
                time, data, header = file.to_edge_ml()
                if time is None: # Dataset empty
                    continue

                # Process each time-series in the dataset
                for d, h in zip(data, header):
                    tsId = ObjectId()
                    tsIds.append(tsId)
                    binStore = BinaryStore(tsId)
                    start, end, sampling_rate, length = binStore._appendValues(time, d)
                    starts.append(start)
                    ends.append(end)
                    sampling_rates.append(sampling_rate)
                    lengths.append(length)
                    headers.append(h)
                    file_names.append(f_info.name)
            asyncDB.setStatus_finished(processId)
        except Exception as e:
            # Delete all saved datasets when there is an exception
            asyncDB.setError(processId, e)
            for tsId in tsIds:
                BinaryStore(tsId).delete()
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR)


        dataset_labeling = [{"labelingId": newLabeling["_id"], "labels": dataset_labels}] if labeling else []
        timeSeries = [{"start": s, "end": e, "_id": tid, "name": fName + "_" + h, "samplingRate": s_rate, "length": l} for s, e, tid, fName, h, s_rate, l in zip(starts, ends, tsIds, file_names, headers, sampling_rates, lengths)]
        dataset = {"name": dataset_name, "userId": userId, "projectId": projectId, "start": min(starts), "end": max(ends), "timeSeries": timeSeries,
        "labelings": dataset_labeling, "metaData": info.metaData}
        try:
            newDatasetMeta = dbm.addDataset(dataset)
        except Exception as e:
            for tsId in tsIds:
                BinaryStore(tsId).delete();
                asyncDB.setError(processId, e)
                raise e
        return True
    except Exception as e:
        print("Error", e)
        asyncDB.setError(processId, e)



def registerDownload(fileInfo, files, projectId, userId, background_tasks):
    id = "%06x" % random.randint(0, 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF)
    asyncDB.add_upload_request(UploadRequest(_id=id))
    background_tasks.add_task(_processData, fileInfo, files, projectId, userId, id)
    return id


def get_status(id):
    status = asyncDB.getStatus(id);
    return status