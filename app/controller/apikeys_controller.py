"""Device-API key management controller.

Ports the behaviour of the backend service (backend/controller/deviceApi.js):
generating, retrieving and removing the per-user read/write API keys of a
project and toggling the device-api activation flag.
"""
import secrets
from typing import Any, Dict

from bson.objectid import ObjectId

from db.project import ProjectDBManager
from db.deviceAPi import DeviceApiManager
from utils import message_queue  # noqa: F401  (kept for parity with backend)

project_dbm = ProjectDBManager()
deviceApi_dbm = DeviceApiManager()


class ApiKeyError(Exception):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.status_code = status_code


def switch_active(project_id: str, user_id: str, state: bool) -> Dict[str, Any]:
    project = project_dbm.project_collection.find_one(
        {"_id": ObjectId(project_id), "admin": ObjectId(user_id)}
    )
    if project is None:
        raise ApiKeyError("No access to this project")

    project_dbm.project_collection.update_one(
        {"_id": project["_id"]}, {"$set": {"enableDeviceApi": bool(state)}}
    )
    return {
        "message": f"DeviceApi for project {project['_id']}: {bool(state)}"
    }


def set_api_key(project_id: str, user_id: str) -> Dict[str, Any]:
    read_api_key = secrets.token_hex(16)
    write_api_key = secrets.token_hex(16)
    existing = deviceApi_dbm.col.find_one(
        {"projectId": ObjectId(project_id), "userId": ObjectId(user_id)}
    )
    if existing:
        deviceApi_dbm.col.update_one(
            {"_id": existing["_id"]},
            {"$set": {"readApiKey": read_api_key, "writeApiKey": write_api_key}},
        )
    else:
        deviceApi_dbm.col.insert_one(
            {
                "projectId": ObjectId(project_id),
                "userId": ObjectId(user_id),
                "readApiKey": read_api_key,
                "writeApiKey": write_api_key,
            }
        )
    return {"readApiKey": read_api_key, "writeApiKey": write_api_key}


def get_api_key(project_id: str, user_id: str):
    doc = deviceApi_dbm.col.find_one(
        {"projectId": ObjectId(project_id), "userId": ObjectId(user_id)}
    )
    if doc:
        return {"readApiKey": doc.get("readApiKey"), "writeApiKey": doc.get("writeApiKey")}
    return {"readApiKey": None, "writeApiKey": None}


def remove_key(project_id: str, user_id: str) -> Dict[str, Any]:
    result = deviceApi_dbm.col.delete_one(
        {"projectId": ObjectId(project_id), "userId": ObjectId(user_id)}
    )
    if result.deleted_count == 0:
        raise ApiKeyError("No access to this project")
    return {"message": "Disabled device api"}
