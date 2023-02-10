from app.db.labelings import LabelingDBManager
from bson.objectid import ObjectId

dbm = LabelingDBManager()


def getProjectLabelings(project_id):
    return dbm.get(projectId=project_id)

def createLabeling(project_id, labeling):
    labeling["projectId"] = ObjectId(project_id)
    return dbm.create(project_id, labeling=labeling)

def updateLabeling(project, id, labeling):
    return dbm.update(project, id, labeling)

def deleteLabeling(project, id):
    return dbm.delete(project, id)

def deleteProjectLabeling(project):
    return dbm.deleteProject(project)