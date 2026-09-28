from fastapi import APIRouter
from routers import dataset, deviceApi, label, labelings, csv
from routers import projects, devices, apikeys, arduino_firmware

router = APIRouter()

router.include_router(
    csv.router,
    prefix="/download"
)

router.include_router(
    dataset.router,
    prefix='/datasets',
    tags=["datasets"]
)

router.include_router(
    label.router,
    prefix='/datasets/labelings',
)

router.include_router(
    deviceApi.router,
    prefix="/api"
)

router.include_router(
    labelings.router,
    prefix="/labelings"
)

router.include_router(
    projects.router,
    prefix="/projects",
    tags=["projects"]
)

router.include_router(
    devices.router,
    prefix="/devices",
    tags=["devices"]
)

router.include_router(
    apikeys.router,
    prefix="/deviceApi",
    tags=["deviceApi"]
)

router.include_router(
    arduino_firmware.router,
    prefix="/arduinoFirmware",
    tags=["arduinoFirmware"]
)