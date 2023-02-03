from app.db.dataset import DatasetDBManager

dbm = DatasetDBManager()

def createLabel(dataset_id, project_id, labelingId, label):
    dataset = dbm.getDatasetById(dataset_id, project_id)
    containsLabeling = any([x["labelingId"] == labelingId for x in dataset["labelings"]])
    if not containsLabeling:
        dataset["labelings"].append({"labels": [], "labelingId": labelingId})
    for l in dataset["labelings"]:
        print(l)
        if l["labelingId"] == labelingId:
            l["labels"].append(label)
    print(dataset)
    dbm.updateDataset(dataset_id, project_id, dataset)