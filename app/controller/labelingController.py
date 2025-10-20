from app.db.labelings import LabelingDBManager
from app.db.dataset import DatasetDBManager
from bson.objectid import ObjectId

import traceback

dbm = LabelingDBManager()
dbm_dataset = DatasetDBManager()

def onLabelingChanged(projectId, labeling):
    labeling_id = ObjectId(labeling["_id"])
    datasets = dbm_dataset.getDatasetsInProjet(projectId)
    labelIds = [ObjectId(x["_id"]) for x in labeling["labels"]]
    for dataset in datasets:
        for (i, labeling) in enumerate(dataset["labelings"]):
            if ObjectId(labeling["labelingId"]) != labeling_id:
                continue
            dataset["labelings"][i]["labels"] = [x for x in labeling["labels"] if (ObjectId(x["type"]) in labelIds)]
        dataset["labelings"] = [x for x in dataset["labelings"] if len(x["labels"]) > 0]
        dbm_dataset.updateDataset(dataset["_id"], projectId, dataset)


def onLabelingDeleted(projectId, labeling_id):
    datasets = dbm_dataset.getDatasetsInProjet(projectId)
    for dataset in datasets:
        dataset["labelings"] = [x for x in dataset["labelings"] if ObjectId(x["labelingId"]) != ObjectId(labeling_id)]
        dbm_dataset.updateDataset(dataset["_id"], projectId, dataset)

def getProjectLabelings(project_id):
    return dbm.get(projectId=project_id)

def createLabeling(project_id, labeling):
    labeling["projectId"] = ObjectId(project_id)
    return dbm.create(project_id, labeling=labeling)

def updateLabeling(project, id, labeling):
    newLabeling = dbm.update(project, id, labeling)
    onLabelingChanged(project, newLabeling)
    return newLabeling

def deleteLabeling(project, id):
    res = dbm.delete(project, id)
    onLabelingDeleted(project, id)
    return res

def deleteProjectLabeling(project):
    return dbm.deleteProject(project)