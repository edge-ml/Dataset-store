"""Projects router.

Implements the `/api/projects` endpoints the frontend expects, previously
served by the Node backend service (backend/routing/routes/project.js).
"""
from typing import Any, Dict

from fastapi import APIRouter, Depends, Response
from fastapi.param_functions import Body
from fastapi import status
from utils.json_encoder import JSONEncoder

from controller.projects_controller import (
    ProjectError,
    create_project,
    delete_project_by_id,
    get_project_by_id,
    get_project_custom_meta_data,
    get_project_sensor_streams,
    get_projects,
    leave_project_by_id,
    update_project_by_id,
)
from routers.dependencies import validate_user, validate_user_no_project
import json

router = APIRouter()


def _json(payload, status_code: int = 200) -> Response:
    return Response(
        json.dumps(payload, cls=JSONEncoder),
        media_type="application/json",
        status_code=status_code,
    )


@router.get("/")
def list_projects(user_id=Depends(validate_user_no_project)):
    return _json(get_projects(str(user_id)))


@router.get("/{project_id}")
def single_project(project_id, user_id=Depends(validate_user)):
    (user_oid, _, _) = user_id
    return _json(get_project_by_id(project_id, str(user_oid)))


@router.get("/{project_id}/sensorStreams")
def project_sensor_streams(project_id, user_id=Depends(validate_user)):
    (user_oid, _, _) = user_id
    return _json(get_project_sensor_streams(project_id, str(user_oid)))


@router.get("/{project_id}/customMetaData")
def project_custom_meta_data(project_id, user_id=Depends(validate_user)):
    (user_oid, _, _) = user_id
    return _json(get_project_custom_meta_data(project_id, str(user_oid)))


@router.post("/", status_code=status.HTTP_201_CREATED)
def create(body: Dict[str, Any] = Body(...), user_id=Depends(validate_user_no_project)):
    try:
        document = create_project(body, str(user_id))
    except ProjectError as error:
        return _json({"error": str(error)}, error.status_code)
    return _json(document, 201)


@router.put("/{project_id}")
def update(project_id, body: Dict[str, Any] = Body(...), user_id=Depends(validate_user)):
    (user_oid, _, _) = user_id
    try:
        result = update_project_by_id(project_id, body, str(user_oid))
    except ProjectError as error:
        return _json({"error": str(error)}, error.status_code)
    return _json(result)


@router.delete("/{project_id}")
def delete(project_id, user_id=Depends(validate_user)):
    (user_oid, _, _) = user_id
    try:
        result = delete_project_by_id(project_id, str(user_oid))
    except ProjectError as error:
        return _json({"message": str(error)}, error.status_code)
    return _json(result)


@router.delete("/{project_id}/leave")
def leave(project_id, user_id=Depends(validate_user)):
    (user_oid, _, _) = user_id
    try:
        result = leave_project_by_id(project_id, str(user_oid))
    except ProjectError as error:
        if error.status_code == 404:
            return _json({"error": str(error)}, 400)
        return _json({"error": str(error)}, error.status_code)
    return _json(result)
