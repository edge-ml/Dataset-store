"""Arduino firmware router.

Implements the `/api/arduinoFirmware/:deviceName` endpoint the frontend
expects, previously served by the Node backend service
(backend/routing/routes/arduinoFirmware.js): downloads the nightly build zip
from GitHub, extracts the first entry (the .bin firmware) and streams it back.
"""
import io
import logging
import zipfile

import requests as http_client
from fastapi import APIRouter, Depends, Response

from routers.dependencies import validate_user_no_project

router = APIRouter()

ALLOWED_DEVICES = ["nicla"]

FIRMWARE_URL = (
    "https://nightly.link/edge-ml/EdgeML-Arduino/workflows/build/main/{device}.bin.zip"
)

logger = logging.getLogger("dataset-store.firmware")


@router.get("/{device_name}")
def get_firmware(device_name: str, user_id=Depends(validate_user_no_project)):
    if device_name not in ALLOWED_DEVICES:
        return Response(
            content='{"message": "Device ' + device_name + ' is not supported"}',
            media_type="application/json",
            status_code=400,
        )
    try:
        response = http_client.get(FIRMWARE_URL.format(device=device_name), timeout=30)
        response.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            first_entry = archive.infolist()[0]
            data = archive.read(first_entry)
    except Exception as exc:
        logger.error("Failed to fetch firmware for %s: %s", device_name, exc)
        return Response(status_code=502)
    return Response(content=data, media_type="application/octet-stream")
