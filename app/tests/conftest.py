"""Pytest configuration.

- Configures all env vars the app reads from `internal/config` BEFORE anything
  from the app package is imported.
- Replaces pymongo.MongoClient with an in-memory fake so no database is needed.
"""
import os
import sys
import tempfile

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
APP_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, APP_DIR)

_TMP_FS = tempfile.mkdtemp(prefix="tsdata_")
_TMP_RAW = tempfile.mkdtemp(prefix="rawupload_")

os.environ.update({
    "MONGO_URI": "mongodb://localhost:27017/",
    "PROJECT_DBNAME": "backend_test",
    "PROJECT_COLLNAME": "projects",
    "DATASTORE_DBNAME": "dataset_store_test",
    "DATASTORE_COLLNAME": "datasets",
    "TIMESERIES_DBNAME": "ts_db_test",
    "TIMESERIES_COLLNAME": "timeSeries",
    "LABELING_COLLNAME": "labelings",
    "DEVICE_API_COLLNAME": "deviceapis",
    "CSV_COLLNAME": "csv_downloads",
    "ASYNC_UPLOAD_COLNAME": "async_upload",
    "SECRET_KEY": "test-secret-key",
    # Pin the users db so a developer's local .env (e.g. DATABASE_COLLECTION_AUTH
    # pointing at "auth_live") cannot leak into the test run.
    "DATABASE_COLLECTION_AUTH": "auth_test",
    "TS_STORE_MECHANISM": "FS",
    "TSDATA": _TMP_FS,
    "RAW_UPLOAD_DATA": _TMP_RAW,
})

import pytest  # noqa: E402
import jwt as pyjwt  # noqa: E402
import pymongo  # noqa: E402
from bson.objectid import ObjectId  # noqa: E402

import mongomock  # noqa: E402

fake_client = mongomock.MongoClient()


def _reset_client():
    """Clear all data while keeping collection handles held by app managers valid."""
    for db_name in fake_client.list_database_names():
        fake_client.drop_database(db_name)


fake_client.reset = _reset_client
pymongo.MongoClient = lambda *a, **k: fake_client  # patch before app imports


from internal.config import (  # noqa: E402
    PROJECT_DBNAME, PROJECT_COLLNAME, DATASTORE_DBNAME, DATASTORE_COLLNAME,
    LABELING_COLLNAME, DEVICE_API_COLLNAME, CSV_COLLNAME, ASYNC_UPLOAD_COLNAME,
    SECRET_KEY,
)


class Seeder:
    """Helper to seed the fake mongo with commonly needed documents."""

    def __init__(self):
        self.project_id = ObjectId()
        self.admin_id = ObjectId()

    @property
    def projects(self):
        return fake_client[PROJECT_DBNAME][PROJECT_COLLNAME]

    @property
    def datasets(self):
        return fake_client[DATASTORE_DBNAME][DATASTORE_COLLNAME]

    @property
    def labelings(self):
        return fake_client[DATASTORE_DBNAME][LABELING_COLLNAME]

    @property
    def deviceapis(self):
        return fake_client[PROJECT_DBNAME][DEVICE_API_COLLNAME]

    @property
    def downloads(self):
        return fake_client[DATASTORE_DBNAME][CSV_COLLNAME]

    @property
    def async_uploads(self):
        return fake_client[DATASTORE_DBNAME][ASYNC_UPLOAD_COLNAME]

    @property
    def users_auth(self):
        from internal.config import AUTH_DBNAME
        return fake_client[AUTH_DBNAME]["users"]

    # -- seeders -------------------------------------------------------------
    def auth_user(self, user_id=None, user_name=None):
        """Seed a user account in the auth collection so projects can resolve it."""
        uid = user_id or ObjectId()
        if user_name is None:
            user_name = f"user-{str(uid)[-6:]}"
        self.users_auth.update_one(
            {"_id": uid},
            {"$set": {"userName": user_name, "email": f"{user_name}@edge-ml.test"}},
            upsert=True,
        )
        return str(uid)

    def project(self, admin=None, users=()):
        self.projects.insert_one({
            "_id": self.project_id,
            "name": "test-project",
            "admin": admin or self.admin_id,
            "users": list(users),
        })
        return str(self.project_id)

    def api_key(self, key="writekey", access="write"):
        doc = {"projectId": self.project_id, "userId": ObjectId()}
        if access == "write":
            doc["writeApiKey"] = key
        else:
            doc["readApiKey"] = key
        self.deviceapis.insert_one(doc)
        return key

    def token(self, user_id=None, expired=False, no_exp=False, key=None):
        import datetime
        payload = {
            "mail": "tester@edge-ml.com",
            "userName": "tester",
            "id": str(user_id or self.admin_id),
            "subscriptionLevel": "standard",
            "iat": datetime.datetime.now(datetime.timezone.utc),
        }
        if not no_exp:
            offset = -60 if expired else 3600
            payload["exp"] = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=offset)
        return pyjwt.encode(payload, key or SECRET_KEY, algorithm="HS256")

    def headers(self, user_id=None, project=None, **kwargs):
        return {
            "project": str(project or self.project_id),
            **kwargs,
        }

    def cookies(self, user_id=None, **kwargs):
        return {"jwt": self.token(user_id=user_id, **kwargs)}

    def dataset(self, time_series=None, name="ds", labelings=None, start=None, end=None):
        ts = time_series if time_series is not None else [
            {"name": "a", "start": 1000, "end": 2000},
        ]
        for t in ts:
            t.setdefault("_id", ObjectId())
        doc = {
            "_id": ObjectId(),
            "name": name,
            "projectId": self.project_id,
            "userId": ObjectId(),
            "metaData": {},
            "start": start if start is not None else 1000,
            "end": end if end is not None else 2000,
            "timeSeries": ts,
            "labelings": labelings or [],
        }
        self.datasets.insert_one(doc)
        return doc


@pytest.fixture(autouse=True)
def _clean_fake_mongo():
    import shutil
    fake_client.reset()
    for d in (_TMP_FS, _TMP_RAW):
        if os.path.isdir(d):
            shutil.rmtree(d, ignore_errors=True)
            os.makedirs(d)
    yield
    fake_client.reset()


@pytest.fixture
def seeder():
    return Seeder()


@pytest.fixture
def client():
    from fastapi.testclient import TestClient
    from main import app
    with TestClient(app) as c:
        yield c


def run_coro(coro):
    import asyncio
    return asyncio.run(coro)
