from fastapi import Depends
from app.utils.jwt import verify_token
from app.utils.project_header import project_header
from app.features.Auth.models import AuthModel_DB
from app.features.Projects.models import ProjectModel_DB
from app.features.Datasets.models import DatasetModel_DB, DatasetModel_Input, TimeSeries
from app.tools.BinaryStore import BinaryStore
from beanie import PydanticObjectId
from fastapi import UploadFile
import rich
import orjson
import pandas as pd
import numpy as np
import random
from fastapi import HTTPException, status
from app.utils.CsvParser import CsvParser
from app.features.Labelings.models import LabelingModel_DB

def _splitMeta_Data(timeSeries):
    tsValues = timeSeries.data
    metaData = timeSeries
    del metaData.data
    if isinstance(tsValues, zip):
        tsValues = list(tsValues)
    return metaData, tsValues


async def get_all_datasets(auth_user: AuthModel_DB, project: ProjectModel_DB):
    datasets = await DatasetModel_DB.find_many(DatasetModel_DB.project.id == project.id).to_list()
    return datasets

async def get_single_dataset(auth_user: AuthModel_DB, project: ProjectModel_DB, id: PydanticObjectId):
    dataset = await DatasetModel_DB.find_one(DatasetModel_DB.id == PydanticObjectId(id), DatasetModel_DB.project.id == project.id)
    return dataset

async def create_dataset(auth_user: AuthModel_DB, project: ProjectModel_DB, data: DatasetModel_Input):
    # First create all the time-series in the dataset
    rich.print(data)
    final_ts = []
    try:
        for ts_input in data.timeSeries:
            ts = TimeSeries(**ts_input.dict())
            bin_store = BinaryStore(ts.id)
            metaData, tsValues = _splitMeta_Data(ts_input)
            start, end, sampling_rate, length = bin_store.append(tsValues)
            ts.start = int(start)
            ts.end = int(end)
            ts.samplingRate = sampling_rate
            ts.length = length
            final_ts.append(ts)
        dataset_db = DatasetModel_DB(
            project=project,
            name=data.name,
            metaData=data.metaData,
            timeSeries=final_ts,
            labelings=data.labelings
        )
        await dataset_db.insert()
    except Exception as e:
        raise e
    

async def getDataSetByIdStartEnd(id, project, start, end, max_resolution):
        dataset = await DatasetModel_DB.find_one(DatasetModel_DB.id == PydanticObjectId(id), DatasetModel_DB.project.id == PydanticObjectId(project.id))

        ts_ids = [x.id for x in dataset.timeSeries]
        res = []
        for t in ts_ids:
            binStore = BinaryStore(t)
            binStore.loadSeries()
            d = binStore.getPart(start, end, max_resolution)
            res.append(d)
        return res

async def getDatasetTimeSeriesStartEnd(id, ts_id, project, start, end, max_resolution):
        dataset = await DatasetModel_DB.find_one(DatasetModel_DB.id == PydanticObjectId(id), DatasetModel_DB.project.id == PydanticObjectId(project.id))
        ts_ids = [x.id for x in dataset.timeSeries]
        if PydanticObjectId(ts_id) not in ts_ids:
            raise Exception("Time series not found in dataset")
        binStore = BinaryStore(PydanticObjectId(ts_id))
        binStore.loadSeries()
        d = binStore.getPart(start, end, max_resolution)
        return d

async def getDatasetInProjectWithPagination(self, projectId, skip, limit, sort, includeTimeseriesData=False):

    sort_map = {
        "alphaDesc": {"field": "name", "order": -1},
        "alphaAsc": {"field": "name", "order": 1},
        "dateDesc": {"field": "timeSeries.start", "order": -1},
        "dateAsc": {"field": "timeSeries.start", "order": 1}
    }

    sortingOptions = {
        "alphaAsc": ("name", 1),
        "alphaDesc": ("name", -1),
        "dateAsc": ("timeSeries.start", 1),
        "dateDesc": ("timeSeries.start", -1)
    }

    # total_count = self.ds_collection.count_documents(DatasetModel_DB.project.id == PydanticObjectId(projectId))
    # datasets = DatasetModel_DB.find(DatasetModel_DB.project.id == PydanticObjectId(projectId), skip=skip, limit=limit, sort=sort_map["sort"])
    total_count = await DatasetModel_DB.find(DatasetModel_DB.project.id == PydanticObjectId(projectId)).count()
    datasets = await DatasetModel_DB.find(DatasetModel_DB.project.id == PydanticObjectId(projectId)).to_list()

    datasets = list(datasets)
    rich.print(datasets)
    if not includeTimeseriesData:
        return {"datasets": datasets, "total_datasets": total_count}
    
    new_datasets = self._populateLabelings(datasets, projectId)
    return {"datasets": new_datasets, "total_datasets": total_count}


async def create_new_csv_dataset(file: UploadFile, file_config, project, auth_user):
        file_config = orjson.loads(file_config)
        name = file_config['name'] if file_config['name'] else (
            file.filename[:-4] if file.filename.endswith('.csv') else file.filename)
        df = pd.read_csv(file.file)
        df.columns = df.columns.str.strip()
        parser = CsvParser(df=df)
        timestamps, sensor_data, label_data, sensor_names, labeling_label_list, labelings, units = parser.to_edge_ml_format(file_config)

        if sensor_data is None:
            raise HTTPException(status.HTTP_400_BAD_REQUEST,
                                detail="The file has no data")

        # look up table to get id and labeling id it belongs from label name
        label_id_labeling = {}
        for labeling in labelings.keys():
            # format labels for the current labeling in loop
            labelsInDBFormat = [{
                'name': label,
                'color': "#%06x" % random.randint(0, 0xFFFFFF),
                'isNewLabel': True
            } for label in labelings[labeling]]

            
            labeling_db = LabelingModel_DB(name=labeling, project=project, labels=labelsInDBFormat)
            await labeling_db.insert()
            for label in labeling_db.labels:
                label_name = label.name
                if label_name not in label_id_labeling:
                    label_id_labeling[label_name] = {'labelingId': labeling_db.id, '_id': label.id}

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

        dataset = {
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
        metadata = await create_dataset(auth_user=auth_user, project=project, data=DatasetModel_Input(**dataset))
        return metadata