"""Device-API key management router.

Implements the `/api/deviceApi` endpoints the frontend expects (setKey /
getKey / deleteKey / switchActive), previously served by the Node backend
service (backend/routing/routes/deviceApi.js).

Note: the *device-facing* API-key endpoints (uploading datasets with an API
key) live in `routers/deviceApi.py` under `/ds/api`.
"""
from typing import Union

from fastapi import APIRouter, Depends, Header, Response
from pydantic import BaseModel

import json
from utils.json_encoder import JSONEncoder

from controller.apikeys_controller import (
    ApiKeyError,
    get_api_key,
    remove_key,
    set_api_key,
    switch_active,
)
from routers.dependencies import validate_user

router = APIRouter()


class SwitchActiveBody(BaseModel):
    state: bool


def _json(payload, status_code: int = 200) -> Response:
    return Response(
        json.dumps(payload, cls=JSONEncoder),
        media_type="application/json",
        status_code=status_code,
    )


@router.get("/setKey")
@router.get("/setkey")
def set_keys(project: str = Header(...), user_id=Depends(validate_user)):
    (user_oid, _, _) = user_id
    return _json(set_api_key(project, str(user_oid)))


@router.get("/getKey")
@router.get("/getkey")
def get_keys(project: str = Header(...), user_id=Depends(validate_user)):
    (user_oid, _, _) = user_id
    result = get_api_key(project, str(user_oid))
    if result is None:
        return _json({"readApiKey": None, "writeApiKey": None})
    return _json(result)


@router.get("/deleteKey")
@router.get("/deletekey")
def delete_keys(project: str = Header(...), user_id=Depends(validate_user)):
    (user_oid, _, _) = user_id
    try:
        result = remove_key(project, str(user_oid))
    except ApiKeyError as error:
        return _json({"error": str(error)}, error.status_code)
    return _json(result)


@router.post("/switchActive")
def switch_active_route(body: SwitchActiveBody, project: str = Header(...), user_id=Depends(validate_user)):
    (user_oid, _, _) = user_id
    try:
        result = switch_active(project, str(user_oid), body.state)
    except ApiKeyError as error:
        return _json({"error": str(error)}, error.status_code)
    return _json(result)
