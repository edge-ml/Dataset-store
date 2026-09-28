import uvicorn
import argparse
from contextlib import asynccontextmanager
from routers import dataset, deviceApi, label, labelings, csv
from routers import auth
from routers import projects, devices, apikeys, arduino_firmware
import logging
import time

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
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("dataset-store")

# Configure boto3 and botocore loggers for detailed debugging
logging.getLogger('boto3').setLevel(logging.DEBUG)
logging.getLogger('botocore').setLevel(logging.DEBUG)
logging.getLogger('s3transfer').setLevel(logging.DEBUG) # For S3 transfer details

class DatasetStore(FastAPI):
    
    def __init__(self, *args, **kwargs):

        app_info = {
            "title": "edge-ml dataset-store"
        }

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

        # Auth (previously provided by the standalone authentication service)
        self.include_router(
            auth.router,
            prefix="/auth",
            tags=["Auth"]
        )
        self.include_router(
            auth.router,
            prefix="/ds/auth",
            include_in_schema=False,
        )

        # Routes migrated from the Node backend service (frontend API_URI)
        self.include_router(
            projects.router,
            prefix="/api/projects",
            tags=["Projects"]
        )
        self.include_router(
            devices.router,
            prefix="/api/devices",
            tags=["Devices"]
        )
        self.include_router(
            apikeys.router,
            prefix="/api/deviceApi",
            tags=["DeviceApiKeys"]
        )
        self.include_router(
            arduino_firmware.router,
            prefix="/api/arduinoFirmware",
            tags=["ArduinoFirmware"]
        )


app = DatasetStore()

rabbitMQTask = None

@app.exception_handler(ValueError)
async def value_error_exception_handler(request: Request, exc: ValueError):
    logger.error(f"ValueError: {exc}")
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"message": str(exc)},
    )
@app.exception_handler(TypeError)
async def type_error_exception_handler(request: Request, exc: TypeError):
    logger.error(f"TypeError: {exc}")
    logger.error(traceback.format_exc())
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST, content={"message": "Invalid input"}
    )

@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Start request path={request.url.path}")
    start_time = time.time()
    response = await call_next(request)
    duration = time.time() - start_time
    logger.info(f"Completed request path={request.url.path} duration={duration:.4f}s status_code={response.status_code}")
    return response


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the database-store")
    parser.add_argument('--env', default="dev", choices=["dev", "docker"])
    parser.add_argument("--num_workers", type=int, default=20, help="Number of workers for uvicorn")
    args = parser.parse_args()
    if args.env == "dev":
        uvicorn.run("main:app", host="0.0.0.0", port=3004, reload=True,
                    proxy_headers=True, forwarded_allow_ips="*")
    if args.env == "docker":
        uvicorn.run("main:app", host="0.0.0.0", port=3004, workers=args.num_workers,
                    proxy_headers=True, forwarded_allow_ips="*")
