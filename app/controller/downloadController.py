from app.db.csv import csvDB
import random
import io
import zipfile
from fastapi.responses import StreamingResponse
from fastapi import BackgroundTasks, FastAPI
from app.controller.dataset_controller import DatasetController
import traceback
import time
from fastapi import HTTPException


downloadData = {}
ctrl = DatasetController()

# Function to check and delete items older than 1 hour from downloadData
def delete_old_items():
    current_time = time.time()
    keys_to_delete = []
    for key, value in downloadData.items():
        if current_time - value['timestamp'] > 3600:  # 1 hour = 3600 seconds
            keys_to_delete.append(key)
    for key in keys_to_delete:
        del downloadData[key]

# Register a task to periodically delete old items
def schedule_delete_task():
    while True:
        delete_old_items()
        time.sleep(60)  # Sleep for 1 hour before running the task again

# Start the task in the background
background_task = BackgroundTasks()
background_task.add_task(schedule_delete_task)

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    # Start the background task when the app starts up
    background_task()

@app.post("/register")
def registerForDownload(project, datasets, background_tasks):
    id = "%06x" % random.randint(0, 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF) 
    downloadData[id] = {"status": 0, "type": "zip" if len(datasets) > 1 else "csv", "timestamp": time.time()}
    background_tasks.add_task(download, id, datasets, project)
    return id


def download(downloadId, datasets, project):
    fileNameCtr = {}
    if len(datasets) > 1:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "a",
                    zipfile.ZIP_DEFLATED, False) as file:
            for id in datasets:
                fileCSV, fileName = ctrl.getCSV(project, id)
                if fileName in fileNameCtr:
                    oldName = fileName
                    fileName = fileName + "_" + str(fileNameCtr[fileName])
                    fileNameCtr[oldName] = fileNameCtr[oldName] + 1
                else:
                    fileNameCtr[fileName] = 1
                file.writestr(fileName, fileCSV.getvalue())        
        downloadData[downloadId]["status"] = 100
        downloadData[downloadId]["data"] = zip_buffer
        return file
    else:
        file, fileName = ctrl.getCSV(project, datasets[0])
        downloadData[downloadId]["status"] = 100
        downloadData[downloadId]["data"] = file
        downloadData[downloadId]["fileName"] = fileName
        return file
    
from fastapi import HTTPException

async def get_status(id):
    try:
        print([k for k, x in downloadData.items()])
        print("Status:", downloadData[id]["status"])
        return downloadData[id]["status"]
    except KeyError:
        raise HTTPException(status_code=404, detail="Download-id not valid")

    

async def get_download_data(id):
    if downloadData[id]["status"] < 100:
        raise Exception("File not ready yet")
    data = downloadData[id]["data"]
    if downloadData[id]["type"] == "zip":
        response = StreamingResponse(iter([data.getvalue()]), media_type="application/zip")
        response.headers["Content-Disposition"] = f"attachment; filename=all.zip"
        return response
    else:
        fileName = downloadData[id]["fileName"]
        response = StreamingResponse(iter([data.getvalue()]), media_type="text/csv")
        response.headers["Content-Disposition"] = f"attachment; filename={fileName}.csv"
        return response