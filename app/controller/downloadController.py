from app.db.csv import csvDB
import random
import io
import zipfile
from fastapi.responses import StreamingResponse
from app.controller.dataset_controller import DatasetController
import traceback


downloadDB = csvDB()
ctrl = DatasetController()

def registerForDownload(project, datasets):
    id = "%06x" % random.randint(0, 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF) 
    downloadDB.add(datasets, project, id)
    return id

async def download(id):
    req = downloadDB.get(id)
    datasets = req.datasets
    project = req.project
    fileNameCtr = {}
    if len(datasets) > 1:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "a",
                    zipfile.ZIP_DEFLATED, False) as file:
            for id in datasets:
                fileCSV, fileName = await ctrl.getCSV(project, id)
                if fileName in fileNameCtr:
                    oldName = fileName
                    fileName = fileName + "_" + str(fileNameCtr[fileName])
                    fileNameCtr[oldName] = fileNameCtr[oldName] + 1
                else:
                    fileNameCtr[fileName] = 1
                file.writestr(fileName, fileCSV.getvalue())
        response = StreamingResponse(iter([zip_buffer.getvalue()]), media_type="application/zip")
        response.headers["Content-Disposition"] = f"attachment; filename=all.zip"
        return response
    else:
        file, fileName = await ctrl.getCSV(project, datasets[0])
        response = StreamingResponse(iter([file.getvalue()]), media_type="text/csv")
        response.headers["Content-Disposition"] = f"attachment; filename={fileName}.csv"
        return response