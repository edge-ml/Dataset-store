"""Projects controller.

Ports the behaviour of the backend service (backend/controller/projects.js)
into the dataset-store. User-name resolution is done directly against the
local users collection instead of an HTTP call to the authentication service.
"""
import re
from typing import Any, Dict, List

from bson.objectid import ObjectId
from pymongo.errors import DuplicateKeyError

from db.project import ProjectDBManager
from db.dataset import DatasetDBManager
from controller.auth_controller import users_dbm
from utils import message_queue

project_dbm = ProjectDBManager()
dataset_dbm = DatasetDBManager()

NAME_PATTERN = re.compile(r"^[\w, -]+$")

# fields non-admin members are allowed to see on a project
NON_ADMIN_FIELDS = ("name", "_id", "admin", "enableDeviceApi")


class ProjectError(Exception):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.status_code = status_code


def _validate_name(name: Any) -> str:
    if not isinstance(name, str) or not NAME_PATTERN.fullmatch(name):
        raise ProjectError("Invalid project name")
    return name


def _normalize_users(users: Any, admin_id: str) -> List[str]:
    """Accept user entries as plain ids or objects carrying an `_id`."""
    if users is None:
        users = []
    if not isinstance(users, list):
        raise ProjectError("Users must be a list")
    normalized = []
    for entry in users:
        uid = entry.get("_id") if isinstance(entry, dict) else entry
        if not isinstance(uid, str):
            try:
                uid = str(uid)
            except Exception:
                raise ProjectError("Invalid user id in users list")
        normalized.append(uid)
    if admin_id in normalized:
        raise ProjectError("Admin cannot be a user of the project")
    if len(set(normalized)) != len(normalized):
        raise ProjectError("Users must be unique")
    return normalized


def _filter_project_non_admin(user_id: str, project: Dict[str, Any]) -> Dict[str, Any]:
    if user_id == str(project["admin"]):
        return project
    return {
        field: project.get(field, False if field == "enableDeviceApi" else None)
        for field in NON_ADMIN_FIELDS
    }


def _cleanup_and_resolve(projects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Resolve admin/user ids against the users collection.

    Mirrors `addUserNamesAndCleanProject` from the backend: projects whose
    admin no longer exists are removed; unknown members are dropped.
    Returns documents with plain string ids for admin/users.
    """
    result = []
    for project in projects:
        admin_id = str(project["admin"])
        member_ids = [str(u) for u in project.get("users", [])]

        known = users_dbm.names_for_ids([admin_id] + member_ids)
        admin_doc = known[0] if known else None

        if admin_doc is None or admin_doc.get("userName") is None:
            # admin vanished from the auth db -> remove the orphaned project
            project_dbm.project_collection.delete_one({"_id": project["_id"]})
            continue

        existing_users = [uid for uid, doc in zip(member_ids, known[1:]) if doc.get("userName") is not None]
        if existing_users != member_ids:
            project_dbm.project_collection.update_one(
                {"_id": project["_id"]}, {"$set": {"users": [ObjectId(u) for u in existing_users]}}
            )

        project["admin"] = admin_id
        project["users"] = existing_users
        result.append(project)
    return result


def get_projects(user_id: str) -> List[Dict[str, Any]]:
    projects = list(
        project_dbm.project_collection.find(
            {"$or": [{"admin": ObjectId(user_id)}, {"users": ObjectId(user_id)}]}
        )
    )
    projects = _cleanup_and_resolve(projects)
    return [_filter_project_non_admin(user_id, p) for p in projects]


def create_project(body: Dict[str, Any], user_id: str) -> Dict[str, Any]:
    name = _validate_name(body.get("name"))
    users = _normalize_users(body.get("users"), user_id)
    document = {
        "name": name,
        "admin": ObjectId(user_id),
        "users": [ObjectId(u) for u in users],
        "enableDeviceApi": bool(body.get("enableDeviceApi", False)),
    }
    try:
        if project_dbm.project_collection.find_one({"name": name, "admin": ObjectId(user_id)}):
            raise ProjectError("A project with this name already exists")
        result = project_dbm.project_collection.insert_one(document)
    except DuplicateKeyError:
        raise ProjectError("A project with this name already exists")
    document["_id"] = result.inserted_id
    return document


def _get_project_or_fail(project_id: str) -> Dict[str, Any]:
    try:
        oid = ObjectId(project_id)
    except Exception:
        raise ProjectError("Cannot find this project", 404)
    project = project_dbm.project_collection.find_one({"_id": oid})
    if project is None:
        raise ProjectError("Cannot find this project", 404)
    return project


def delete_project_by_id(project_id: str, user_id: str) -> Dict[str, Any]:
    project = project_dbm.project_collection.find_one(
        {"_id": ObjectId(project_id), "admin": ObjectId(user_id)}
    ) if _is_object_id(project_id) else None
    if project is None:
        raise ProjectError("Cannot delete this project")

    project_dbm.project_collection.delete_one({"_id": project["_id"]})
    # notify downstream services (ml, dataset cleanup) about the deletion
    message_queue.publish("projectDelete", str(project["_id"]))
    return {"message": f"deleted project with id: {project_id}"}


def leave_project_by_id(project_id: str, user_id: str) -> Dict[str, Any]:
    if not _is_object_id(project_id):
        raise ProjectError("Cannot find this project", 404)
    result = project_dbm.project_collection.update_one(
        {"_id": ObjectId(project_id), "users": ObjectId(user_id)},
        {"$pull": {"users": ObjectId(user_id)}},
    )
    if result.matched_count == 0:
        raise ProjectError("Cannot leave this project")
    return {"message": "removed user"}


def update_project_by_id(project_id: str, body: Dict[str, Any], user_id: str) -> Dict[str, Any]:
    if not _is_object_id(project_id):
        raise ProjectError("Cannot find this project", 404)
    project = project_dbm.project_collection.find_one(
        {"_id": ObjectId(project_id), "admin": ObjectId(user_id)}
    )
    if project is None:
        raise ProjectError("No access to this project", 404)

    update = {}
    if "name" in body:
        update["name"] = _validate_name(body.get("name"))
    if "users" in body:
        update["users"] = [ObjectId(u) for u in _normalize_users(body.get("users"), user_id)]
    if "enableDeviceApi" in body:
        update["enableDeviceApi"] = bool(body.get("enableDeviceApi"))

    try:
        if update:
            if "name" in update and project_dbm.project_collection.find_one(
                {"name": update["name"], "admin": ObjectId(user_id), "_id": {"$ne": project["_id"]}}
            ):
                raise ProjectError("A project with this name already exists")
            project_dbm.project_collection.update_one({"_id": project["_id"]}, {"$set": update})
    except DuplicateKeyError:
        raise ProjectError("A project with this name already exists")
    return {"message": f"updated project with id: {project_id}"}


def get_project_by_id(project_id: str, user_id: str) -> Dict[str, Any]:
    if not _is_object_id(project_id):
        raise ProjectError("Cannot find this project", 404)
    project = project_dbm.project_collection.find_one(
        {
            "_id": ObjectId(project_id),
            "$or": [{"admin": ObjectId(user_id)}, {"users": ObjectId(user_id)}],
        }
    )
    if project is None:
        raise ProjectError("Cannot find this project", 404)
    (resolved,) = _cleanup_and_resolve([project])
    return _filter_project_non_admin(user_id, resolved)


def get_project_sensor_streams(project_id: str, user_id: str) -> Dict[str, Any]:
    get_project_by_id(project_id, user_id)  # access check
    datasets = dataset_dbm.getDatasetsInProjet(project_id)
    names = set()
    for dataset in datasets:
        for ts in dataset.get("timeSeries", []):
            names.add(ts["name"])
    return {"sensorStreams": sorted(names)}


def get_project_custom_meta_data(project_id: str, user_id: str) -> Dict[str, Any]:
    get_project_by_id(project_id, user_id)  # access check
    datasets = list(dataset_dbm.getDatasetsInProjet(project_id))
    keys = []
    for dataset in datasets:
        for key in (dataset.get("metaData") or {}).keys():
            if key not in keys:
                keys.append(key)
    frequency = {key: 0 for key in keys}
    for dataset in datasets:
        for key in (dataset.get("metaData") or {}).keys():
            frequency[key] += 1
    return {"metaDataKeys": keys, "metaDataKeyFrequency": frequency}


def _is_object_id(value: str) -> bool:
    try:
        ObjectId(value)
        return True
    except Exception:
        return False
