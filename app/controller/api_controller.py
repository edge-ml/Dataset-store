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
        print(timeSeries)
        newStart = dataset["start"]
        newEnd = dataset["end"]
        for ts_name, ts_data in timeSeries.items():
            id = name_to_id[ts_name]
            binStore = BinaryStore(id)
            start, end = binStore.append(ts_data)
            index = next((i for i, item in enumerate(dataset["timeSeries"]) if item["_id"] == id), -1)
            dataset["timeSeries"][index]["start"] = start
            dataset["timeSeries"][index]["end"] = end
            newStart = min(newStart, start)
            newEnd = max(newEnd, end)
        dataset["start"] = newStart
        dataset["end"] = newEnd
        dbm.updateDataset(dataset["_id"], projectId, dataset)
    except Exception as e:
        print(e)
        print(traceback.format_exc())
    
        