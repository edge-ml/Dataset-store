from app.db.dataset import DatasetDBManager
from bson.objectid import ObjectId

dbm = DatasetDBManager()

def createLabel(dataset_id, project_id, labelingId, label):
    dataset = dbm.getDatasetById(dataset_id, project_id)
    containsLabeling = any([ObjectId(x["labelingId"]) == ObjectId(labelingId) for x in dataset["labelings"]])
    if not containsLabeling:
        dataset["labelings"].append({"labels": [], "labelingId": labelingId})
    for l in dataset["labelings"]:
        if ObjectId(l["labelingId"]) == ObjectId(labelingId):
            if not "_id" in label:
                label["_id"] = ObjectId()
            l["labels"].append(label)
    dbm.updateDataset(dataset_id, project_id, dataset)
    return label

def updateLabel(project_id, dataset_id, labeling_id, label_id, updatedLabel):
    print(updatedLabel)
    dataset = dbm.getDatasetById(dataset_id, project_id)
    for i, labeling in enumerate(dataset["labelings"]):
        if str(labeling["labelingId"]) == str(labeling_id):
            for j, label in enumerate(labeling["labels"]):
                if str(label["_id"]) == str(label_id):
                    dataset["labelings"][i]["labels"][j] = updatedLabel
    dbm.updateDataset(dataset_id, project_id, dataset)

def deleteLabel(project_id, dataset_id, labeling_id, label_id):
    dataset = dbm.getDatasetById(dataset_id, project_id)
    for i, labeling in enumerate(dataset["labelings"]):
        if str(labeling["labelingId"]) == str(labeling_id):
            for j, label in enumerate(labeling["labels"]):
                print(label)
                if str(label["_id"]) == str(label_id):
                    del dataset["labelings"][i]["labels"][j]
    dbm.updateDataset(dataset_id, project_id, dataset)