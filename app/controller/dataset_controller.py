from io import StringIO
from app.db.dataset import DatasetDBManager, ProgressStep
from .binary_store import BinaryStore
from bson.objectid import ObjectId
import os
from fastapi import HTTPException, status
from app.utils.helpers import PyObjectId, custom_index
from app.db.deviceAPi import DeviceApiManager
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
import numpy as np
import bson

class FileDescriptor(BaseModel):
    name: str
    size: int
    drop: List[str]
    time: List[str]

class CSVLabel(BaseModel):
    start: str
    end: str
    name: str
    metaData: Optional[Dict[str, str]] = Field(default={})

class CsvLabeling(BaseModel):
    name: str
    labels: List[CSVLabel]

class CSVDatasetInfo(BaseModel):
    name: str
    files: List[FileDescriptor]
    labeling: CsvLabeling | None = None
    metaData: Dict[str, str] | None = None
    saveRaw: bool = Field(default=False)

class EdgeMLCSVDatasetInfo(BaseModel):
    name: str
    files: List[FileDescriptor]
class DatasetController():

    def __init__(self):

        if not os.path.exists("DATA"):
            os.mkdir("DATA")

        self.dbm = DatasetDBManager()
        self.deviceAPI = DeviceApiManager()
        self.dbm_labeling = LabelingDBManager()
        self.csv_processings = {} # dataset_id - progress step


    def _splitMeta_Data(self, timeSeries):
        tsValues = timeSeries["data"]
        metaData = timeSeries
        del metaData["data"]
        return metaData, tsValues

    def _convertObejctIdsToStr(self, data):
        data["projectId"] = str(data["projectId"])
        data["_id"] = str(data["_id"])
        for i, t in enumerate(data["timeSeries"]):
            data["timeSeries"][i]["_id"] = str(t["_id"])
        return data

    def getDatasetById(self, dataset_id, project, onlyMeta=False):
        # Read dataset from database
        datasetMeta = self.dbm.getDatasetById(dataset_id, project)
        if onlyMeta:
            return datasetMeta
        for t in datasetMeta["timeSeries"]:
            binStore = BinaryStore(t["_id"])
            binStore.loadSeries()
            data = binStore.getFull()
            t["data"] = [[x, y] for x, y in zip(data["time"].tolist(), data["data"].tolist())]
        return datasetMeta



    def addDataset(self, dataset, project, user_id=None):
        datasetMeta = dataset
        datasetMeta["projectId"] = ObjectId(project)
        if user_id is not None:
            datasetMeta["userId"] = ObjectId(user_id)
            if dataset.get("progressStep") == ProgressStep.PARSING.value:
                newDatasetMeta = self.dbm.addDataset(datasetMeta)
            else:
                newDatasetMeta = self.dbm.updateDataset(datasetMeta["_id"], project, datasetMeta)
        try:
            totalTimeSeries = min(len(datasetMeta["timeSeries"]), len(newDatasetMeta["timeSeries"]))
            for i, (t, newt) in enumerate(zip(datasetMeta["timeSeries"], newDatasetMeta["timeSeries"])):
                metaData, tsValues = self._splitMeta_Data(t)
                if isinstance(tsValues, zip):
                    tsValues = list(tsValues)
                binStore = BinaryStore(newt["_id"])
                start, end, sampling_rate, length = binStore.append(tsValues)
                newDatasetMeta["timeSeries"][i]["start"] = start
                newDatasetMeta["timeSeries"][i]["end"] = end
                newDatasetMeta["timeSeries"][i]["length"] = length
                newDatasetMeta["timeSeries"][i]["samplingRate"] = sampling_rate
                self.dbm.partialUpdate(newDatasetMeta["_id"], newDatasetMeta["projectId"], {"progressStep": ProgressStep.UPLOADING_DATASET.value + [i + 1, totalTimeSeries]})
            newDatasetMeta = self.dbm.updateDataset(newDatasetMeta["_id"], newDatasetMeta["projectId"], newDatasetMeta)
        except Exception as e:
            self.dbm.deleteDatasetById(project, newDatasetMeta["_id"])
            raise e
        return newDatasetMeta

    def _convertTimeSeriesObjectIdToStr(self, ts_array):
        res = []
        for t in ts_array:
            t["_id"] = str(t["_id"])
            res.append(t)
        return res

    def getDatasetInProject(self, projectId, includeTimeseriesData=False):
        datasets = self.dbm.getDatasetsInProjet(projectId)
        datasets = list(datasets)
        if not includeTimeseriesData:
            return datasets
        
        labelings = self.dbm_labeling.get(projectId)
        labeling_mapping = {entry['_id']: entry['name'] for entry in labelings}
        label_mapping = {}
        for entry in labelings:
            for label in entry['labels']:
                label_mapping[label['_id']] = label['name']
        datasets_with_timeseries = []
        for dataset in datasets:
            ds = self.getDatasetById(dataset['_id'], projectId, False)
            ds['labelings'] = [{'labeling': labeling_mapping[labeling['labelingId']], 
                    'labels': [{'start': label['start'], 
                                'end': label['end'], 
                                'label': label_mapping[label['type']]}
                               for label in labeling['labels']]}
                   for labeling in ds['labelings']]
            datasets_with_timeseries.append(ds)
        return datasets_with_timeseries

    def getTimeSeriesData(self, projectId, datasetId, timeSeriesId):
        dataset = self.dbm.getDatasetById(datasetId, projectId)
        
        binStore = BinaryStore(timeSeriesId)
        return binStore.getHdf5Stream()



    def deleteDataset(self, id, projectId):
        ts_ids = self.dbm.deleteDatasetById(projectId, id)
        for id in ts_ids:
            binStore = BinaryStore(id)
            binStore.delete()

    def renameDataset(self, id, projectId, newName):
        return self.dbm.partialUpdate(id, projectId, {"name": newName})
    
    def updateUnitConfig(self, dataset_id, timeSeries_id, project_id, unit, scaling, offset):
        scaling = float(scaling)
        offset = float(offset)
        self.dbm.updateTimeSeriesUnit(dataset_id, timeSeries_id, project_id, unit)
        binStore = BinaryStore(timeSeries_id)
        binStore.loadSeries()
        d = binStore.getFull()
        data = d["data"] * scaling + offset
        binStore.data_arr = data
        binStore.saveSeries()
        return True


    def updateDataset(self, id, projectId, dataset):
        return self.dbm.updateDataset(id, projectId, dataset)

    def getDataSetByIdStartEnd(self, id, projectId, start, end, max_resolution):
        dataset = self.dbm.getDatasetById(id, project_id=projectId)
        ts_ids = [x["_id"] for x in dataset["timeSeries"]]
        res = []
        for t in ts_ids:
            binStore = BinaryStore(t)
            binStore.loadSeries()
            d = binStore.getPart(start, end, max_resolution)
            res.append(d)
        return res

    def getDatasetTimeSeriesStartEnd(self, dataset_id, ts_id, project_id, start, end, max_resolution):
        dataset = self.dbm.getDatasetById(dataset_id, project_id=project_id)
        dataset_ids = [str(x["_id"]) for x in dataset["timeSeries"]]
        res = []
        for t in ts_id:
            if str(t) not in dataset_ids:
                raise HTTPException(status.HTTP_404_NOT_FOUND)
            binStore = BinaryStore(t)
            binStore.loadSeries()
            res.append(binStore.getPart(start, end, max_resolution))
        return res

    def append(self, id, project, body, projectId):
        dataset = self.dbm.getDatasetById(id, project)
        datasetIds = [x["_id"] for x in dataset["timeSeries"]]
        sendIds = [ObjectId(x["_id"]) for x in body["timeSeries"]]
        if set(datasetIds) != set(sendIds):
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Authentication failed")

        for ts in body['timeSeries']:
            binStore = BinaryStore(ts["_id"])
            binStore.loadSeries()
            tmpStart, tmpEnd, sampling_rate, length  = binStore.append(ts["data"])
            idx = custom_index(dataset["timeSeries"], lambda x: ObjectId(x["_id"]) == ObjectId(ts["_id"]))
            oldStart = dataset["timeSeries"][idx]["start"]
            oldEnd = dataset["timeSeries"][idx]["end"]
            dataset["timeSeries"][idx]["start"] = min(oldStart, tmpStart) if oldStart is not None else tmpStart
            dataset["timeSeries"][idx]["end"] = max(oldEnd, tmpEnd) if oldEnd is not None else tmpEnd
            dataset["timeSeries"][idx]["length"] = length
            dataset["timeSeries"][idx]["samplingRate"] = sampling_rate
        
        labelings = []
        for label in body["labels"]:
            labeling = next((l for l in labelings if l["labelingId"] == label["labelingId"]), None)
            if labeling is None:
                labelings.append({"labelingId": label["labelingId"], 
                                  "labels": [{
                                      "start": label["start"], 
                                      "end": label["end"], 
                                      "type": label["labelType"], "metadata": {}
                                }]})
            else:
                labeling["labels"].append({
                    "start": label["start"],
                    "end": label["end"],
                    "type": label["labelType"],
                    "metadata": {}
                    })
        
        for labeling in labelings:
            dataset_labeling = next((l for l in dataset["labelings"] if ObjectId(labeling["labelingId"]) == l["labelingId"]), None)
            if dataset_labeling is None:
                dataset["labelings"].append(labeling)
            elif dataset_labeling["labels"][-1]["end"] == labeling["labels"][0]["start"]:
                dataset_labeling["labels"][-1]["end"] = labeling["labels"][0]["start"]
                dataset_labeling["labels"].extend(labeling["labels"][1:])
            else:
                dataset_labeling["labels"].extend(labeling["labels"])
        
        self.dbm.updateDataset(id, project, dataset=dataset)
        return

    @staticmethod
    def generate_dataset_id():
        return str(bson.ObjectId())
    
    def get_upload_progress(self, dataset_id: str, project_id: str):
        return self.dbm.getDatasetById(dataset_id, project_id)['progressStep']
    
    def CSVUpload(self, file: UploadFile, config: dict, project: str, user_id: str, dataset_id: PyObjectId):
        name = config['name'] if config['name'] else (
            file.filename[:-4] if file.filename.endswith('.csv') else file.filename)
        dataset = {
            '_id': ObjectId(dataset_id), 
            'name': name, 
            'projectId': project, 
            'userId': ObjectId(user_id),
            'progressStep': ProgressStep.PARSING.value
        }
        metadata = self.dbm.addDataset(dataset)
        df = pd.read_csv(file.file)
        df.columns = df.columns.str.strip()
        parser = CsvParser(df=df)
        timestamps, sensor_data, label_data, sensor_names, labeling_label_list, labelings, units = parser.to_edge_ml_format(config)

        if sensor_data is None:
            raise HTTPException(status.HTTP_400_BAD_REQUEST,
                                detail="The file has no data")

        self.dbm.partialUpdate(id=dataset_id, project_id=project, updates={'progressStep': ProgressStep.LABELING.value})
        # look up table to get id and labeling id it belongs from label name
        label_id_labeling = {}
        for labeling in labelings.keys():
            # format labels for the current labeling in loop
            labelsInDBFormat = [{
                'name': label,
                'color': "#%06x" % random.randint(0, 0xFFFFFF),
                'isNewLabel': True
            } for label in labelings[labeling]]

            labelingInDB = createLabeling(project, {"name": labeling, "labels": labelsInDBFormat})
            for label in labelingInDB['labels']:
                label_name = label['name']
                if label_name not in label_id_labeling:
                    label_id_labeling[label_name] = {'labelingId': labelingInDB['_id'], '_id': label['_id']}
        
        labelingsInDatasetFormat = {}
        for label_idx, data in enumerate(label_data):
            idx = 0
            assert len(data) == len(timestamps), 'Label column length does not match timestamp column length'
            data_length = len(data)
            # intervals for the current label
            intervals = []
            # labeling_label_list has the following format: labeling_label
            # extract only label
            label_name = labeling_label_list[label_idx].split('_')[1]
            labelingId = label_id_labeling[label_name]['labelingId']
            while idx < data_length:
                if data[idx] == 'x':
                    start = timestamps[idx]
                    while idx < data_length and data[idx] == 'x':
                        idx += 1
                    end = timestamps[idx - 1]
                    intervals.append((start, end))                    
                idx += 1
            if labelingId not in labelingsInDatasetFormat:
                    labelingsInDatasetFormat[labelingId] = []
            for start, end in intervals:
                labelingsInDatasetFormat[labelingId].append({
                    'type': label_id_labeling[label_name]['_id'],
                    'start': start,
                    'end': end,
                })
        
        labelingsInDatasetFormat = [{
            'labelingId': labelingId,
            'labels': labelingsInDatasetFormat[labelingId],
        } for labelingId in labelingsInDatasetFormat.keys()]
        self.dbm.partialUpdate(id=dataset_id, project_id=project, updates={'progressStep': ProgressStep.CREATING_DATASET.value})
        dataset = {
            '_id': ObjectId(dataset_id),
            'name': name,
            'timeSeries': [{
                'name': sensor,
                'start': timestamps[0],
                'end': timestamps[-1],
                'unit': units[sensor_idx],
                'data': np.column_stack((timestamps, sensor_data.iloc[:, sensor_idx]))
            } for sensor_idx, sensor in enumerate(sensor_names)],
            'labelings': labelingsInDatasetFormat
        }
        self.dbm.partialUpdate(id=dataset_id, project_id=project, updates={'progressStep': ProgressStep.UPLOADING_DATASET.value})
        metadata = self.addDataset(dataset, project=project, user_id=user_id)
        self.dbm.partialUpdate(id=dataset_id, project_id=project, updates={'progressStep': ProgressStep.COMPLETE.value})
        return metadata

    def externalUpload(self, api_key, user_id, body):
        # Get labels from dataset
        dataset = body
        datasetLabels = body["labeling"]["labels"]
        labelingName = body["labeling"]["name"]
        uniquelabels = list(set([x["name"] for x in datasetLabels]))

        # Check if api_key has access rights
        deviceApi = self.deviceAPI.get(api_key)
        project_id = deviceApi["projectId"]
        dataset["userId"] = deviceApi["userId"]

        color_dict = {x: "#%06x" % random.randint(0, 0xFFFFFF) for x in uniquelabels}

        for l in datasetLabels:
            l["color"] = color_dict[l["name"]]
        labeling = createLabeling(project_id, {"name": labelingName, "labels": datasetLabels})
        typeDict = {x["name"]: x["_id"] for x in labeling["labels"]}
        for l in datasetLabels:
            l["type"] = typeDict[l["name"]]
        dataset["labelings"] = [{"labelingId": labeling["_id"], "labels": datasetLabels}]
        metadataset = self.addDataset(dataset=dataset, project=project_id, user_id=user_id)
        return metadataset["_id"]

    def deleteProjectDatasets(self, project):
        datasets = self.dbm.getDatasetsInProjet(project)

        for dataset in datasets:
            timeSeriesIDs = [x["_id"] for x in dataset["timeSeries"]]

            for d in timeSeriesIDs:
                binStore = BinaryStore(d)
                binStore.delete()
        self.dbm.deleteProject(project)


    # Upload whole datasets
    async def receiveFileInfoAndCSV(self, websocket, projectId, userId, dataModel = CSVDatasetInfo):
        transmitting = True
        files_byte = bytearray()
        while transmitting:
            info = await websocket.receive_text()
            info = json.loads(info)
            info = dataModel.parse_obj(info)
            total_size = sum([x.size for x in info.files])
            while True:
                data = await websocket.receive_bytes()
                files_byte += data
                if len(files_byte) == total_size:
                    transmitting = False
                    break
        return info, files_byte


    def generateLabeling(self, projectId, labeling : CsvLabeling):
        unique_labels_names = [x.name for x in labeling.labels]
        unique_labels = [{"name": x, "color": f'#{"%06x" % random.randint(0, 0xFFFFFF)}'} for x in unique_labels_names]
        return createLabeling(projectId, {"name": labeling.name, "labels": unique_labels})



    async def uploadDatasetDevice(self, info, files, projectId, userId):
        try:
            info = CSVDatasetInfo.parse_obj(info)
            dataset_name = info.name
            labeling = info.labeling
            file_info = info.files

            # Add new labeling to the db
            if labeling:
                print("using labels")
                newLabeling = self.generateLabeling(projectId=projectId, labeling=labeling)

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
            except Exception as e:
                # Delete all saved datasets when there is an exception
                for tsId in tsIds:
                    BinaryStore(tsId).delete()
                raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR)


            dataset_labeling = [{"labelingId": newLabeling["_id"], "labels": dataset_labels}] if labeling else []
            timeSeries = [{"start": s, "end": e, "_id": tid, "name": fName + "_" + h, "samplingRate": s_rate, "length": l} for s, e, tid, fName, h, s_rate, l in zip(starts, ends, tsIds, file_names, headers, sampling_rates, lengths)]
            dataset = {"name": dataset_name, "userId": userId, "projectId": projectId, "start": min(starts), "end": max(ends), "timeSeries": timeSeries,
            "labelings": dataset_labeling, "metaData": info.metaData}
            try:
                newDatasetMeta = self.dbm.addDataset(dataset)
            except:
                for tsId in tsIds:
                    BinaryStore(tsId).delete();
                    raise e
            return True
        except Exception as e:
            print("Error", e)
            traceback.print_exc()
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR)


    def getCSV(self, projectId, dataset_id):
        dataset = self.dbm.getDatasetById(dataset_id, projectId)
        fileName = dataset["name"]
        timeSeries = dataset["timeSeries"]
        final_df = None
        for ts in timeSeries:
            binStore = BinaryStore(ts["_id"])
            binStore.loadSeries()
            ts_data = binStore.getFull()
            df = pd.DataFrame(ts_data)
            if final_df is None:
                final_df = df
            else:
                final_df = pd.merge(final_df, df, how="outer", on='time')
            final_df.columns = [*final_df.columns[:-1], "sensor_" + ts["name"]]

        final_df.set_index("time", inplace=True)
        
        # Add labelings
        for labeling in dataset["labelings"]:
            # Get labeling from db
            labeling_definition = self.dbm_labeling.get_single(projectId, labeling["labelingId"])
            labeling_name = labeling_definition["name"]
            for label in labeling_definition["labels"]:
                dataset_labels = filter(lambda x: x["type"] == label["_id"], labeling["labels"])
                newLabelCol = "label_" + labeling_name + "_" + label["name"]
                final_df[newLabelCol] = ""
                for dataset_label in dataset_labels:
                    start = int(dataset_label["start"])
                    end = int(dataset_label["end"])
                    final_df.loc[start:end, newLabelCol] = "x"
        
        textStream = StringIO()
        final_df.to_csv(textStream, index=True)
        return textStream, fileName