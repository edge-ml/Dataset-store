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
from app.internal.config import RAW_UPLOAD_DATA
import shutil


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
    saveRaw: bool = Field(default=False)


def generateLabeling(projectId, labeling : CsvLabeling):
    unique_labels_names = [x.name for x in labeling.labels]
    unique_labels = [{"name": x, "color": f'#{"%06x" % random.randint(0, 0xFFFFFF)}'} for x in unique_labels_names]
    return createLabeling(projectId, {"name": labeling.name, "labels": unique_labels})

async def _processData(info, files : List[UploadFile], projectId, userId, processId):
    try:
        info = CSVDatasetInfo.parse_obj(info)
        dataset_name = info.name
        labeling = info.labeling
        file_info = info.files

        # Write the raw data to disk
        # Undocumented feature
        saveFolderPath = None
        if info.saveRaw:
            if not os.path.exists(RAW_UPLOAD_DATA):
                os.makedirs(RAW_UPLOAD_DATA)

            saveFolderPath = os.path.join(RAW_UPLOAD_DATA, info.name)
            print(saveFolderPath, os.path.exists(saveFolderPath))
            if not os.path.exists(saveFolderPath):
                # Create folder
                os.makedirs(saveFolderPath)

                # Write metadata:
                with open(os.path.join(saveFolderPath, "metadata.json"), "w") as file:
                    file.write(json.dumps(info.dict(by_alias=True), indent=4))
                
                # Write the data
                for file in files:
                    filePath = os.path.join(saveFolderPath, file.filename)
                    with open(filePath, "wb") as f:
                        while True:
                            chunk = await file.read(1024)
                            if not chunk:
                                break
                            f.write(chunk)
                        file.seek(0)
            else:
                raise Exception("Folder already exists")

        # Add new labeling to the db
        if labeling:
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
                await files[i].seek(0)
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
            if saveFolderPath is not None:
                shutil.rmtree(saveFolderPath)
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR)

        try:
            dataset_labeling = [{"labelingId": newLabeling["_id"], "labels": dataset_labels}] if labeling else []
            timeSeries = [{"start": s, "end": e, "_id": tid, "name": fName + "_" + h, "samplingRate": s_rate, "length": l} for s, e, tid, fName, h, s_rate, l in zip(starts, ends, tsIds, file_names, headers, sampling_rates, lengths)]
            dataset = {"name": dataset_name, "userId": userId, "projectId": projectId, "start": min(starts), "end": max(ends), "timeSeries": timeSeries,
            "labelings": dataset_labeling, "metaData": info.metaData}
            newDatasetMeta = dbm.addDataset(dataset)
        except Exception as e:
            for tsId in tsIds:
                BinaryStore(tsId).delete();
                asyncDB.setError(processId, e)
                if saveFolderPath is not None:
                    shutil.rmtree(saveFolderPath)
            raise e
        return True
    except Exception as e:
        print("Error", e)
        print(traceback.format_exc())
        asyncDB.setError(processId, str(e))



def registerDownload(fileInfo, files, projectId, userId, background_tasks):
    id = "%06x" % random.randint(0, 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF)
    asyncDB.add_upload_request(UploadRequest(_id=id, user_id=userId))
    background_tasks.add_task(_processData, fileInfo, files, projectId, userId, id)
    return id


def get_status(id, user_id):
    uploadRequest = asyncDB.getStatus(id, user_id)
    if uploadRequest.error == "Folder already exists":
        raise HTTPException(status_code=409, detail=uploadRequest.error)
    if uploadRequest.error != "":
        raise HTTPException(status_code=500, detail=uploadRequest.error)
    return {"status": uploadRequest.status}