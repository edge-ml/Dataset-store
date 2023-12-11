from app.db.dataset import DatasetDBManager
from bson.objectid import ObjectId
from fastapi import HTTPException
from bson.objectid import ObjectId

dbm = DatasetDBManager()

'''
Returns True if the labels are overlapping
'''
def _checkLabelOverlap(dataset, label, labelingId):
    datasetLabeling = next(x for x in dataset["labelings"] if str(x["labelingId"]) == str(labelingId))
    for datasetLabel in datasetLabeling["labels"]:
        if max(datasetLabel["start"], label["start"]) <= min(datasetLabel["end"], label["end"]) and str(label["_id"]) != str(datasetLabel["_id"]):
            return True
    return False
        

def createLabel(dataset_id, project_id, labelingId, label):
    label["start"] = int(label["start"])
    label["end"] = int(label["end"])
    dataset = dbm.getDatasetById(dataset_id, project_id)
    containsLabeling = any([ObjectId(x["labelingId"]) == ObjectId(labelingId) for x in dataset["labelings"]])
    if not containsLabeling:
        dataset["labelings"].append({"labels": [], "labelingId": labelingId})
    for l in dataset["labelings"]:
        if ObjectId(l["labelingId"]) == ObjectId(labelingId):
            if not "_id" in label:
                label["_id"] = ObjectId()
            if _checkLabelOverlap(dataset, label, labelingId):
                raise HTTPException(status_code=400, detail="Labels of the same labeling cannot overlap")
            l["labels"].append(label)
    dbm.updateDataset(dataset_id, project_id, dataset)
    return label

def updateLabel(project_id, dataset_id, labeling_id, label_id, updatedLabel):
    dataset = dbm.getDatasetById(dataset_id, project_id)
    for i, labeling in enumerate(dataset["labelings"]):
        if str(labeling["labelingId"]) == str(labeling_id):
            for j, label in enumerate(labeling["labels"]):
                if str(label["_id"]) == str(label_id):
                    if _checkLabelOverlap(dataset, updatedLabel, labeling_id):
                        raise HTTPException(status_code=400, detail="Labels of the same labeling cannot overlap")
                    dataset["labelings"][i]["labels"][j] = updatedLabel
    dbm.updateDataset(dataset_id, project_id, dataset)

def deleteLabel(project_id, dataset_id, labeling_id, label_id):
    dataset = dbm.getDatasetById(dataset_id, project_id)
    for i, labeling in enumerate(dataset["labelings"]):
        if str(labeling["labelingId"]) == str(labeling_id):
            for j, label in enumerate(labeling["labels"]):
                if str(label["_id"]) == str(label_id):
                    del dataset["labelings"][i]["labels"][j]
                    if len(dataset["labelings"][i]["labels"]) == 0:
                        del dataset["labelings"][i]
    dbm.updateDataset(dataset_id, project_id, dataset)