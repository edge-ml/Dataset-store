import uvicorn
import argparse
from contextlib import asynccontextmanager
from routers import dataset, deviceApi, label, labelings, csv

parser = argparse.ArgumentParser(description="Run the database-store")
parser.add_argument('--env', default="dev", choices=["dev", "docker"])
args = parser.parse_args()
env = args.env

from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from MessageQueue import main
import asyncio
import argparse
from routers import router
from fastapi.middleware.gzip import GZipMiddleware
import traceback


class DatasetStore(FastAPI):
    
    def __init__(self, *args, **kwargs):

        app_info = {
            "title": "edge-ml dataset-store"
        }

        # tags_metadata = {
        #     "name": "datasets",
        #     "description": "Allows to manage datasets"
        # }

        super().__init__(*args, **{**app_info, **kwargs})

        # TODO: adapt to specific origins
        self.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"]
        )

        self.add_middleware(GZipMiddleware, minimum_size=1000)

        self.include_router(
            dataset.router,
            prefix='/ds/datasets',
            tags=["Datasets"]
        )

        self.include_router(
            csv.router,
            prefix="/ds/download",
            tags=["Download"]
        )

        self.include_router(
            label.router,
            prefix='/ds/datasets/labelings',
            tags=["DatasetLabelings"]
        )

        self.include_router(
            deviceApi.router,
            prefix="/ds/api",
            tags=["API"]
        )

        self.include_router(
            labelings.router,
            prefix="/ds/labelings",
            tags=["Labelings"]
        )


app = DatasetStore()

rabbitMQTask = None

@app.exception_handler(ValueError)
async def value_error_exception_handler(request: Request, exc: ValueError):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"message": str(exc)},
    )
@app.exception_handler(TypeError)
async def type_error_exception_handler(request: Request, exc: TypeError):
    print(exc)
    print(traceback.format_exc())
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST, content={"message": "Invalid input"}
    )

@asynccontextmanager
async def lifespan(app: FastAPI):
    # On startup
    global rabbitMQTask
    loop = asyncio.get_event_loop()
    rabbitMQTask = asyncio.create_task(main(loop))
    yield
    # On shutdown
    rabbitMQTask.cancel()


if __name__ == "__main__":
    if env == "dev":
        uvicorn.run("main:app", host="0.0.0.0", port=3004, reload=True)
    if env == "docker":
        uvicorn.run("main:app", host="0.0.0.0", port=3004, workers=20)

