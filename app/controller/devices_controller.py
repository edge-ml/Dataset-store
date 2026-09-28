"""Device & sensor controller.

Ports the behaviour of the backend service (backend/controller/device.js).
Devices and sensors live in the same Mongo database the backend used
(PROJECT_DBNAME, collections `devices` / `sensors`).
"""
from typing import Any, Dict

from bson.objectid import ObjectId
from pymongo import MongoClient

import internal.config as config


class DeviceError(Exception):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.status_code = status_code


class DeviceDBManager:
    def __init__(self) -> None:
        client = MongoClient(config.MONGO_URI)
        db = client[config.PROJECT_DBNAME]
        self.device_collection = db["devices"]
        self.sensor_collection = db["sensors"]


device_dbm = DeviceDBManager()


def _serialize(doc: Dict[str, Any]) -> Dict[str, Any]:
    return doc


def get_devices() -> list:
    return [d for d in device_dbm.device_collection.find({})]


def get_device_by_name_and_generation(name: str, generation: str):
    main_generation = generation.split(".")[0]
    try:
        gen_number = float(main_generation)
    except ValueError:
        raise DeviceError("Invalid generation", 404)
    device = device_dbm.device_collection.find_one(
        {"name": name, "generation": {"$in": [main_generation, gen_number]}}
    )
    if device is None:
        raise DeviceError("Not found", 404)
    sensors = list(device_dbm.sensor_collection.find({"device": device["_id"]}))
    return {"device": device, "sensors": sensors}


def create_device(body: Dict[str, Any]) -> Dict[str, Any]:
    document = {
        "name": body.get("name"),
        "generation": body.get("generation"),
        "maxSampleRate": body.get("maxSampleRate"),
        "user": ObjectId(body["user"]) if body.get("user") else None,
        "sensors": [ObjectId(s) for s in body.get("sensors", [])],
    }
    if not document["name"] or document["generation"] is None or document["maxSampleRate"] is None:
        raise DeviceError("name, generation and maxSampleRate are required")
    result = device_dbm.device_collection.insert_one(document)
    document["_id"] = result.inserted_id
    return document


def update_device_by_id(device_id: str, body: Dict[str, Any]):
    if not _is_object_id(device_id):
        raise DeviceError("Not found", 404)
    update = {k: v for k, v in body.items() if k in ("name", "generation", "maxSampleRate", "user", "sensors")}
    if "sensors" in update:
        update["sensors"] = [ObjectId(s) for s in update["sensors"]]
    if "user" in update and update["user"]:
        update["user"] = ObjectId(update["user"])
    device_dbm.device_collection.update_one({"_id": ObjectId(device_id)}, {"$set": update})
    return {"message": f"updated device with id: {device_id}"}


def _is_object_id(value: str) -> bool:
    try:
        ObjectId(value)
        return True
    except Exception:
        return False
