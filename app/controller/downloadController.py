from app.db.csv import csvDB
import random
import io
import zipfile
from fastapi.responses import StreamingResponse
from fastapi import BackgroundTasks, FastAPI
from app.controller.dataset_controller import DatasetController
from app.db.csv import csvDB, DBEntryDataset, DBEntryProject
from app.db.dataset import DatasetDBManager
from app.db.project import ProjectDBManager
import traceback
import time
from fastapi import HTTPException
import tempfile
import os
from app.utils.helpers import PyObjectId

ctrl = DatasetController()

db = csvDB()
project_db = ProjectDBManager()
dataset_db = DatasetDBManager()

# Function to check and delete items older than 1 hour from downloadData
def delete_old_items():
    print("Cleanup")
    res = db.getOlder(1)
    print(res)
    for r in res:
        os.remove(r.filePath)
        db.delete(r.downloadId)

# Register a task to periodically delete old items
def schedule_delete_task():
    while True:
        delete_old_items()
        print("running")
        time.sleep(5)

# Start the task in the background
background_task = BackgroundTasks()
background_task.add_task(schedule_delete_task)

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    background_task()

def registerForDownloadDataset(projectId, dataset_id, userId, background_tasks):
    id = "%06x" % random.randint(0, 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF)
    project = project_db.get_project(projectId)
    dataset = dataset_db.getDatasetById(dataset_id, projectId)
    data = DBEntryDataset(downloadId=id, projectId=projectId, userId=userId, projectName=project["name"], datasetName=dataset["name"])
    db.add(data)
    background_tasks.add_task(downloadDataset, id, projectId, dataset["_id"])
    return id

def registerForDownloadProject(projectId: PyObjectId, userId: PyObjectId, background_tasks):
    id = "%06x" % random.randint(0, 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF)
    project = project_db.get_project(projectId)
    print(project["name"])
    data = DBEntryProject(downloadId=id, projectId=projectId, userId=userId, projectName=project["name"])
    db.add(data)
    background_tasks.add_task(downloadProject, id, projectId)
    return id


def downloadProject(downloadId, project):
    fileNameCtr = {}
    datasets = dataset_db.getDatasetsInProjet(project)
    datasets = [d["_id"] for d in datasets]
    print("Saving datasets: ", datasets)
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        with zipfile.ZipFile(temp_file, "a", zipfile.ZIP_DEFLATED, False) as file:
            for id in datasets:
                fileCSV, fileName = ctrl.getCSV(project, id)
                if fileName in fileNameCtr:
                    oldName = fileName
                    fileName = fileName + "_" + str(fileNameCtr[fileName]) + ".csv"
                    fileNameCtr[oldName] = fileNameCtr[oldName] + 1
                else:
                    fileNameCtr[fileName] = 1
                file.writestr(fileName, fileCSV.getvalue())   
        # Store the temporary file in downloadData
        db.update(download_id=downloadId, status=100, filePath=temp_file.name)

def downloadDataset(downloadId, project, dataset):
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        fileCSV, fileName = ctrl.getCSV(project, dataset)
        temp_file.write(fileCSV.getvalue().encode())
        temp_file.flush()
        db.update(downloadId, status=100, fileName=fileName, filePath=temp_file.name)


async def get_status(user_id):
    try:
        res = db.get_by_user(user_id)
        return res
    except KeyError:
        raise HTTPException(status_code=404, detail="Download-id not valid")

async def get_download_data(id):
    res = db.get(id)
    if res.status < 100:
        raise Exception("File not ready yet")

    print(type(res))

    if isinstance(res, DBEntryProject):
        with open(res.filePath, "rb") as file:
            response = StreamingResponse(iter([file.read()]), media_type="application/zip")
            response.headers["Content-Disposition"] = f"attachment; filename={res.projectName}.zip"
            return response
    else:
        # Open the file as a binary file for reading
        with open(res.filePath, "rb") as file:
            response = StreamingResponse(iter([file.read()]), media_type="text/csv")
            response.headers["Content-Disposition"] = f"attachment; filename={res.fileName}.csv"
            return response

async def cancel_download(downloadId):
    res = db.get(downloadId)
    print(res)
    try:
        os.remove(res.filePath)
    except:
        pass
    db.delete(res.downloadId)