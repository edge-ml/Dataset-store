"""Devices router.

Implements the `/api/devices` endpoints the frontend expects, previously
served by the Node backend service (backend/routing/routes/device.js).
"""
from typing import Any, Dict

from fastapi import APIRouter, Depends, Response
from fastapi.param_functions import Body

from controller.devices_controller import (
    DeviceError,
    create_device,
    get_device_by_name_and_generation,
    get_devices,
    update_device_by_id,
)
from routers.dependencies import validate_user
import json
from utils.json_encoder import JSONEncoder

router = APIRouter()


def _json(payload, status_code: int = 200) -> Response:
    return Response(
        json.dumps(payload, cls=JSONEncoder),
        media_type="application/json",
        status_code=status_code,
    )


@router.get("/")
def list_devices(user_id=Depends(validate_user)):
    return _json(get_devices())


@router.get("/{name}/{generation}")
def device_by_name_and_generation(name, generation, user_id=Depends(validate_user)):
    try:
        result = get_device_by_name_and_generation(name, generation)
    except DeviceError as error:
        return _json({"error": str(error)}, error.status_code)
    return _json(result)


@router.post("/", status_code=201)
def create(body: Dict[str, Any] = Body(...), user_id=Depends(validate_user)):
    try:
        document = create_device(body)
    except DeviceError as error:
        return _json({"error": str(error)}, error.status_code)
    return _json(document, 201)


@router.put("/{device_id}")
def update(device_id, body: Dict[str, Any] = Body(...), user_id=Depends(validate_user)):
    try:
        result = update_device_by_id(device_id, body)
    except DeviceError as error:
        return _json({"error": str(error)}, error.status_code)
    return _json(result)
