from db.csv import csvDB
import random
import io
import zipfile
from fastapi.responses import StreamingResponse
from fastapi import BackgroundTasks, FastAPI
from controller.dataset_controller import DatasetController
from db.csv import csvDB, DBEntryDataset, DBEntryProject
from db.dataset import DatasetDBManager
from db.project import ProjectDBManager
from controller.media_store import mediaStore
import traceback
import time
import asyncio
from fastapi import HTTPException
from fastapi import HTTPException
import tempfile
import os
from utils.helpers import PyObjectId
from contextlib import asynccontextmanager

ctrl = DatasetController()

db = csvDB()
project_db = ProjectDBManager()
dataset_db = DatasetDBManager()

# Function to check and delete items older than 1 hour from downloadData
def delete_old_items():
    res = db.getOlder(60 * 60)
    for r in res:
        if os.path.exists(r.filePath):
            os.remove(r.filePath)
        db.delete(r.downloadId)

# Register a task to periodically delete old items
async def schedule_delete_task(interval=5):
    while True:
        await asyncio.sleep(interval)
        delete_old_items()

# Start the task in the background
app = FastAPI()

@app.on_event("startup")
async def startup_event():
    # Run the cleanup-loop as a background task; awaiting it inline would block
    # the event loop (and therefore the whole application startup) forever.
    asyncio.create_task(schedule_delete_task())

# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     background_task()


def registerForDownloadDataset(projectId, dataset_id, userId, background_tasks):
    id = "%06x" % random.randint(0, 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF)
    project = project_db.get_project(projectId)
    dataset = dataset_db.getDatasetById(dataset_id, projectId)
    data = DBEntryDataset(downloadId=id, projectId=projectId, userId=userId, projectName=project["name"], datasetName=dataset["name"])
    db.add(data)
    background_tasks.add_task(downloadDataset, id, projectId, dataset["_id"])
    return data.dict()

def registerForDownloadProject(projectId: PyObjectId, userId: PyObjectId, background_tasks):
    id = "%06x" % random.randint(0, 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF)
    project = project_db.get_project(projectId)
    data = DBEntryProject(downloadId=id, projectId=projectId, userId=userId, projectName=project["name"])
    db.add(data)
    background_tasks.add_task(downloadProject, id, projectId)
    return data.dict()


def downloadProject(downloadId, project):
    fileNameCtr = {}
    datasets = dataset_db.getDatasetsInProjet(project)
    datasets = [d["_id"] for d in datasets]
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        with zipfile.ZipFile(temp_file, "a", zipfile.ZIP_DEFLATED, False) as file:
            for id in datasets:
                fileCSV, fileName = ctrl.getCSV(project, id)
                if fileName in fileNameCtr:
                    oldName = fileName
                    fileName = fileName + "_" + str(fileNameCtr[fileName])
                    fileNameCtr[oldName] = fileNameCtr[oldName] + 1
                else:
                    fileNameCtr[fileName] = 1
                file.writestr(fileName + ".csv", fileCSV.getvalue())   
        # Store the temporary file in downloadData
        db.update(download_id=downloadId, status=100, filePath=temp_file.name)

# file extension of a recording, by its mime type
MEDIA_EXTENSIONS = {
    "video/mp4": ".mp4", "video/webm": ".webm", "audio/mpeg": ".mp3", "audio/wav": ".wav",
    "audio/ogg": ".ogg", "audio/flac": ".flac", "audio/mp4": ".m4a", "audio/webm": ".webm",
}


def _unique(name, taken):
    """`name`, or `stem_1.ext`, `stem_2.ext`, ... if it is in `taken`. Adds it."""
    stem, ext = os.path.splitext(name)
    candidate, n = name, 0
    while candidate in taken:
        n += 1
        candidate = f"{stem}_{n}{ext}"
    taken.add(candidate)
    return candidate


def _writeDatasetArchive(target, project, dataset, fileCSV, fileName):
    """Everything a dataset with recordings holds: the sensor data, the
    labels as time ranges, and the recordings as they were uploaded."""
    taken = set()
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as archive:
        if dataset.get("timeSeries"):
            archive.writestr(_unique(fileName + ".csv", taken), fileCSV.getvalue())
        labels = ctrl.getLabelsCSV(project, dataset["_id"])
        archive.writestr(_unique(fileName + "_labels.csv", taken), labels.getvalue())
        for media in dataset.get("mediaSeries", []):
            key = str(media["_id"])
            size = mediaStore.size(key)
            if size is None:
                raise FileNotFoundError(f"The file of the recording {media['name']} is missing")
            name = media["name"].replace("/", "_").replace("\\", "_") or media["kind"]
            name = _unique(name + MEDIA_EXTENSIONS.get(media.get("mimeType"), ""), taken)
            # audio and video are compressed already
            info = zipfile.ZipInfo(name, date_time=time.localtime()[:6])
            info.compress_type = zipfile.ZIP_STORED
            with archive.open(info, "w", force_zip64=True) as entry:
                for chunk in mediaStore.read(key, 0, size - 1):
                    entry.write(chunk)


def downloadDataset(downloadId, project, dataset_id):
    path = None
    try:
        dataset = dataset_db.getDatasetById(dataset_id, project)
        fileCSV, fileName = ctrl.getCSV(project, dataset_id)
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            path = temp_file.name
            if dataset.get("mediaSeries"):
                _writeDatasetArchive(temp_file, project, dataset, fileCSV, fileName)
                fileType = "zip"
            else:
                temp_file.write(fileCSV.getvalue().encode())
                fileType = "csv"
        db.update(downloadId, status=100, fileName=fileName, filePath=path, fileType=fileType)
    except Exception as e:
        # tell the waiting user instead of leaving the download at 0 % forever
        print(traceback.format_exc())
        if path and os.path.exists(path):
            os.remove(path)
        db.update(downloadId, error=str(e) or "The download could not be prepared")


async def get_status(user_id):
    try:
        res = db.get_by_user(user_id)
        return res
    except KeyError:
        raise HTTPException(status_code=404, detail="Download-id not valid")

async def get_download_data(id):
    res = db.get(id)
    if res.status < 100:
        raise HTTPException(status_code=409, detail="File not ready yet")

    if isinstance(res, DBEntryProject):
        with open(res.filePath, "rb") as file:
            response = StreamingResponse(iter([file.read()]), media_type="application/zip")
            response.headers["Content-Disposition"] = f"attachment; filename={res.projectName}.zip"
            return response
    else:
        # Open the file as a binary file for reading
        if res.fileType == "zip":
            # may hold large recordings: send it in pieces
            def chunks():
                with open(res.filePath, "rb") as file:
                    while chunk := file.read(1024 * 1024):
                        yield chunk
            response = StreamingResponse(chunks(), media_type="application/zip")
            response.headers["Content-Disposition"] = f"attachment; filename={res.fileName}.zip"
            return response
        with open(res.filePath, "rb") as file:
            response = StreamingResponse(iter([file.read()]), media_type="text/csv")
            response.headers["Content-Disposition"] = f"attachment; filename={res.fileName}.csv"
            return response

async def cancel_download(downloadId):
    res = db.get(downloadId)
    try:
        os.remove(res.filePath)
    except:
        pass
    db.delete(res.downloadId)