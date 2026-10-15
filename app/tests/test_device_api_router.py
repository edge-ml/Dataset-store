"""Tests for the device-API router (/ds/api/...) using a write/read api key."""
import io
import json

import pytest
from bson.objectid import ObjectId

from tests.conftest import fake_client, DATASTORE_DBNAME, DATASTORE_COLLNAME


DEVICE_CSV = b"time,temp\n1690000100.5,1.0\n1690000200.75,2.0\n"


@pytest.fixture
def keys(seeder):
    seeder.project()
    return {
        "write": seeder.api_key("w-key", access="write"),
        "read": seeder.api_key("r-key", access="read"),
    }


def device_info(**overrides):
    info = {
        "name": "device-ds",
        "files": [{"name": "f1.csv", "size": 10, "drop": [], "time": ["time"]}],
        "labeling": None,
        "metaData": {},
        "saveRaw": False,
    }
    info.update(overrides)
    return info


class TestInitDataset:
    def test_init(self, client, keys):
        r = client.post(f"/ds/api/dataset/init/{keys['write']}?api_key={keys['write']}",
                        json={"name": "n", "timeSeries": ["a", "b"], "metaData": {"x": "y"}})
        assert r.status_code == 200
        ds_id = r.json()["id"]
        doc = fake_client[DATASTORE_DBNAME][DATASTORE_COLLNAME].find_one({})
        assert str(doc["_id"]) == ds_id
        assert len(doc["timeSeries"]) == 2


class TestAppend:
    def test_append(self, client, keys):
        r = client.post(f"/ds/api/dataset/init/{keys['write']}?api_key={keys['write']}",
                        json={"name": "n", "timeSeries": ["a"], "metaData": {}})
        ds_id = r.json()["id"]
        body = {"data": [{"name": "a", "data": [[1000000000, 1.5], [2000000000, 2.5]]}]}
        r = client.post(f"/ds/api/dataset/append/{keys['write']}/{ds_id}?api_key={keys['write']}",
                        json=body)
        assert r.status_code == 200


def init(client, key, series=("a", "b")):
    r = client.post(f"/ds/api/dataset/init/{key}?api_key={key}",
                    json={"name": "n", "timeSeries": list(series), "metaData": {}})
    return r.json()["id"]


def append(client, key, ds_id, body, prefix="/ds/api"):
    return client.post(f"{prefix}/dataset/append/{key}/{ds_id}?api_key={key}", json=body)


def labels_of(ds_id):
    doc = fake_client[DATASTORE_DBNAME][DATASTORE_COLLNAME].find_one({"_id": ObjectId(ds_id)})
    return [(l["start"], l["end"]) for labeling in doc["labelings"] for l in labeling["labels"]]


class TestAppendLabels:
    def test_ranged_labels(self, client, keys):
        ds_id = init(client, keys["write"])
        data = [{"name": "a", "data": [[1000, 1.0], [2000, 2.0], [3000, 3.0]]}]
        r = append(client, keys["write"], ds_id, {"data": data, "labels": [
            {"labelingName": "activity", "labelName": "walk", "start": 1000, "end": 1999},
            {"labelingName": "activity", "labelName": "run", "start": 2000, "end": 3000},
        ]})
        assert r.status_code == 200, r.text
        assert sorted(labels_of(ds_id)) == [(1000, 1999), (2000, 3000)]
        labelings = list(fake_client["dataset_store_test"]["labelings"].find({}))
        assert [[l["name"] for l in x["labels"]] for x in labelings] == [["walk", "run"]]

    def test_labels_only_append_and_whole_dataset_label(self, client, keys):
        ds_id = init(client, keys["write"])
        append(client, keys["write"], ds_id, {"data": [{"name": "a", "data": [[1000, 1.0], [5000, 2.0]]}]})
        r = append(client, keys["write"], ds_id, {"labeling": {"labelingName": "activity", "labelName": "sit"}})
        assert r.status_code == 200, r.text
        assert labels_of(ds_id) == [(1000, 5000)]

    def test_errors_are_reported(self, client, keys):
        ds_id = init(client, keys["write"])
        r = append(client, keys["write"], ds_id, {"data": [{"name": "nope", "data": [[1, 1.0]]}]})
        assert r.status_code == 400
        assert "nope" in r.json()["detail"]
        r = append(client, keys["write"], str(ObjectId()), {"data": []})
        assert r.status_code == 404
        r = append(client, keys["write"], ds_id, {"labels": [
            {"labelingName": "L", "labelName": "x", "start": 5, "end": 1}]})
        assert r.status_code == 400
        # overlapping labels of one labeling are rejected by the label controller
        ok = {"labelingName": "L", "labelName": "x", "start": 1, "end": 10}
        assert append(client, keys["write"], ds_id, {"labels": [ok]}).status_code == 200
        assert append(client, keys["write"], ds_id, {"labels": [ok]}).status_code == 400

    def test_legacy_v1_path(self, client, keys):
        ds_id = init(client, keys["write"])
        r = append(client, keys["write"], ds_id, {"data": [{"name": "a", "data": [[1, 1.0]]}]},
                   prefix="/api/v1/deviceapi")
        assert r.status_code == 200, r.text


class TestSyncUpload:
    def test_upload_files(self, client, keys):
        info = json.dumps(device_info())
        r = client.post(
            f"/ds/api/dataset/device/{keys['write']}?api_key={keys['write']}",
            data={"json_data": info},
            files=[("files", ("f1.csv", io.BytesIO(DEVICE_CSV), "text/csv"))])
        assert r.status_code == 200
        assert fake_client[DATASTORE_DBNAME][DATASTORE_COLLNAME].count_documents({}) == 1


class TestGetDatasets:
    def test_read_key(self, client, keys):
        from controller.dataset_controller import DatasetController
        DatasetController().addDataset({"name": "d", "userId": str(ObjectId()),
                                        "timeSeries": [{"name": "a", "data": [[1, 2.0]]}]},
                                       str(fake_client["backend_test"]["projects"].find_one({})["_id"]))
        pid = fake_client["backend_test"]["projects"].find_one({})["_id"]
        # dataset must belong to the key's project
        fake_client[DATASTORE_DBNAME][DATASTORE_COLLNAME].replace_one(
            {}, {**fake_client[DATASTORE_DBNAME][DATASTORE_COLLNAME].find_one({}),
                 "projectId": pid})
        r = client.get(f"/ds/api/datasets/{keys['read']}?includeTimeseriesData=true&api_key={keys['read']}")
        assert r.status_code == 200
        assert len(r.json()) == 1

    def test_write_key_can_read(self, client, keys):
        r = client.get(f"/ds/api/datasets/{keys['write']}?includeTimeseriesData=false&api_key={keys['write']}")
        assert r.status_code == 200

    def test_read_key_cannot_write(self, client, keys):
        r = client.post(f"/ds/api/dataset/init/{keys['read']}?api_key={keys['read']}",
                        json={"name": "n", "timeSeries": ["a"]})
        assert r.status_code == 401
        assert "write" in r.json()["detail"]


class TestAsyncUpload:
    def test_full_async_flow(self, client, keys):
        info = json.dumps(device_info(labeling={
            "name": "L", "labels": [{"start": "1", "end": "2", "name": "on"}]}))
        r = client.post(
            f"/ds/api/async/device/{keys['write']}?api_key={keys['write']}",
            data={"json_data": info},
            files=[("files", ("f1.csv", io.BytesIO(DEVICE_CSV), "text/csv"))])
        assert r.status_code == 202
        upload_id = r.json()["uploadId"]
        assert r.headers["location"] == upload_id

        r = client.get(f"/ds/api/async/device/{keys['write']}/status/{upload_id}?api_key={keys['write']}")
        assert r.headers["x-status"] == "100"

    def test_error_status_maps_to_500(self, client, keys):
        from db.async_device_upload import AsyncUploadDB, UploadRequest
        from tests.conftest import fake_client
        user_id = fake_client["backend_test"]["deviceapis"].find_one({})["userId"]
        AsyncUploadDB().add_upload_request(UploadRequest(_id="u1", user_id=user_id))
        AsyncUploadDB().setError("u1", "kaboom")
        r = client.get(f"/ds/api/async/device/{keys['write']}/status/u1?api_key={keys['write']}")
        assert r.status_code == 500

    def test_folder_exists_maps_to_409(self, client, keys):
        from db.async_device_upload import AsyncUploadDB, UploadRequest
        from tests.conftest import fake_client
        user_id = fake_client["backend_test"]["deviceapis"].find_one({})["userId"]
        db = AsyncUploadDB()
        db.add_upload_request(UploadRequest(_id="u2", user_id=user_id))
        db.setError("u2", "Folder already exists")
        r = client.get(f"/ds/api/async/device/{keys['write']}/status/u2?api_key={keys['write']}")
        assert r.status_code == 409


class TestProjectEndpoints:
    def test_get_project_info_and_h5(self, client, keys):
        from controller.dataset_controller import DatasetController
        pid = str(fake_client["backend_test"]["projects"].find_one({})["_id"])
        meta = DatasetController().addDataset({
            "name": "h5-ds", "userId": str(ObjectId()),
            "timeSeries": [{"name": "a", "data": [[1, 2.0]]}]}, pid)

        r = client.get(f"/ds/api/project/{keys['read']}?api_key={keys['read']}")
        assert r.status_code == 200
        body = r.json()
        assert any(d["name"] == "h5-ds" for d in body["datasets"])

        ts_id = str(meta["timeSeries"][0]["_id"])
        r = client.get(f"/ds/api/project/{keys['read']}/{meta['_id']}/{ts_id}?api_key={keys['read']}")
        assert r.status_code == 200
        assert r.content[:4] != b""  # h5 payload streamed
