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


class _Progress:
    """Reports how far a download is, as a percentage below 100 (100 means
    the file is ready). `total` is the amount of work in any unit."""

    def __init__(self, downloadId, total):
        self.downloadId = downloadId
        self.total = max(total, 1)
        self.done = 0
        self.reported = 0

    def add(self, amount):
        self.done += amount
        percent = min(99, int(self.done * 100 / self.total))
        if percent > self.reported:
            self.reported = percent
            db.update(self.downloadId, status=percent)


# What a dataset costs to export, for the progress: a recording counts by its
# size, the sensor data and labels as a fixed amount (their size is not known
# before they are written).
CSV_WEIGHT = 1024 * 1024


def _weight(dataset):
    return CSV_WEIGHT + sum(m.get("size", 0) for m in dataset.get("mediaSeries", []))


def _writeRecordings(archive, project, dataset, labelsName, folder, taken, progress):
    """The labels of a dataset as time ranges and its recordings as uploaded,
    for a dataset that holds recordings."""
    labels = ctrl.getLabelsCSV(project, dataset["_id"])
    archive.writestr(_unique(labelsName, taken), labels.getvalue())
    for media in dataset.get("mediaSeries", []):
        key = str(media["_id"])
        size = mediaStore.size(key)
        if size is None:
            raise FileNotFoundError(f"The file of the recording {media['name']} is missing")
        name = media["name"].replace("/", "_").replace("\\", "_") or media["kind"]
        name = _unique(folder + name + MEDIA_EXTENSIONS.get(media.get("mimeType"), ""), taken)
        # audio and video are compressed already
        info = zipfile.ZipInfo(name, date_time=time.localtime()[:6])
        info.compress_type = zipfile.ZIP_STORED
        with archive.open(info, "w", force_zip64=True) as entry:
            for chunk in mediaStore.read(key, 0, size - 1):
                entry.write(chunk)
                progress.add(len(chunk))


def _fail(downloadId, path, error):
    # tell the waiting user instead of leaving the download unfinished forever
    print(traceback.format_exc())
    if path and os.path.exists(path):
        os.remove(path)
    db.update(downloadId, error=str(error) or "The download could not be prepared")


def downloadProject(downloadId, project):
    """All datasets of a project in one archive: a CSV per dataset, and for a
    dataset with recordings also its labels as time ranges and, in a folder
    of its name, the recordings."""
    path = None
    try:
        datasets = list(dataset_db.getDatasetsInProjet(project))
        progress = _Progress(downloadId, sum(_weight(d) for d in datasets))
        taken = set()
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            path = temp_file.name
            with zipfile.ZipFile(temp_file, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as archive:
                for dataset in datasets:
                    fileCSV, fileName = ctrl.getCSV(project, dataset["_id"])
                    hasMedia = bool(dataset.get("mediaSeries"))
                    # one name for everything of this dataset, also when
                    # another dataset is called the same
                    name = os.path.splitext(_unique(fileName + ".csv", taken))[0]
                    # without sensor data the CSV of a dataset with recordings is empty
                    if dataset.get("timeSeries") or not hasMedia:
                        archive.writestr(name + ".csv", fileCSV.getvalue())
                    progress.add(CSV_WEIGHT)
                    if hasMedia:
                        _writeRecordings(archive, project, dataset, name + "_labels.csv", name + "/", taken, progress)
        db.update(download_id=downloadId, status=100, filePath=path)
    except Exception as e:
        _fail(downloadId, path, e)


def downloadDataset(downloadId, project, dataset_id):
    """A dataset without recordings as one CSV; with recordings as an archive
    of the sensor data, the labels as time ranges and the recordings."""
    path = None
    try:
        dataset = dataset_db.getDatasetById(dataset_id, project)
        progress = _Progress(downloadId, _weight(dataset))
        fileCSV, fileName = ctrl.getCSV(project, dataset_id)
        progress.add(CSV_WEIGHT)
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            path = temp_file.name
            if dataset.get("mediaSeries"):
                taken = set()
                with zipfile.ZipFile(temp_file, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as archive:
                    if dataset.get("timeSeries"):
                        archive.writestr(_unique(fileName + ".csv", taken), fileCSV.getvalue())
                    _writeRecordings(archive, project, dataset, fileName + "_labels.csv", "", taken, progress)
                fileType = "zip"
            else:
                temp_file.write(fileCSV.getvalue().encode())
                fileType = "csv"
        db.update(downloadId, status=100, fileName=fileName, filePath=path, fileType=fileType)
    except Exception as e:
        _fail(downloadId, path, e)


async def get_status(user_id):
    try:
        res = db.get_by_user(user_id)
        return res
    except KeyError:
        raise HTTPException(status_code=404, detail="Download-id not valid")

def _file(path, media_type, name):
    """The prepared file, sent in pieces."""
    def chunks():
        with open(path, "rb") as file:
            while chunk := file.read(1024 * 1024):
                yield chunk
    headers = {"Content-Disposition": f"attachment; filename={name}"}
    if media_type == "application/zip":
        # An archive is compressed already: send it as it is and with its
        # size, so that the browser can show how far the transfer is. (The
        # gzip middleware would drop the size; it still shrinks a CSV a lot.)
        headers["Content-Length"] = str(os.path.getsize(path))
        headers["Content-Encoding"] = "identity"
    return StreamingResponse(chunks(), media_type=media_type, headers=headers)


async def get_download_data(id):
    res = db.get(id)
    if res.status < 100:
        raise HTTPException(status_code=409, detail="File not ready yet")

    if isinstance(res, DBEntryProject):
        return _file(res.filePath, "application/zip", f"{res.projectName}.zip")
    if res.fileType == "zip":
        return _file(res.filePath, "application/zip", f"{res.fileName}.zip")
    return _file(res.filePath, "text/csv", f"{res.fileName}.csv")

async def cancel_download(downloadId):
    res = db.get(downloadId)
    try:
        os.remove(res.filePath)
    except:
        pass
    db.delete(res.downloadId)