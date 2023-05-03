from app.db.dataset import DatasetDBManager
from app.controller.binary_store import BinaryStore
from app.controller.dataset_controller import DatasetController
import traceback

dbm = DatasetDBManager()
ctrl = DatasetController()

def initDataset(name, timeSeries, metaData, userId, projectId):
    newTimeSeries = [{"name": x, "start": 2**63-1, "end": -1} for x in timeSeries]
    newDataset = {"name": name, "start": 2**63-1, "end": -1, "timeSeries": newTimeSeries, "userId": userId, "projectId": projectId, "metaData": metaData}
    newDataset = dbm.addDataset(newDataset)
    for ts in newDataset["timeSeries"]:
        binStore = BinaryStore(ts["_id"])
        binStore.saveSeries()
    return str(newDataset["_id"])

def appendDataset(timeSeries, userId, projectId, dataset_id):
    try:
        dataset = dbm.getDatasetById(dataset_id=dataset_id, project_id=projectId)
        name_to_id = {x["name"]: x["_id"] for x in dataset["timeSeries"]}
        for ts in timeSeries:
            id = name_to_id[ts.name]
            binStore = BinaryStore(id)
            binStore.loadSeries()
            start, end, sampling_rate, length  = binStore.append(ts.data)
            index = next((i for i, item in enumerate(dataset["timeSeries"]) if item["_id"] == id), -1)
            dataset["timeSeries"][index]["length"] = length
            dataset["timeSeries"][index]["samplingRate"] = sampling_rate
            dataset["timeSeries"][index]["start"] = start
            dataset["timeSeries"][index]["end"] = end
            dataset["start"] = min(dataset["start"], start)
            dataset["end"] = max(dataset["end"], end)
        print("Dataset start: ", dataset["start"])
        dbm.updateDataset(dataset["_id"], projectId, dataset)
    except Exception as e:
        print(e)
        print(traceback.format_exc())
    
        