"""End-to-end router tests against the FastAPI app (Mongo + S3/FS faked)."""
import io
import json

import numpy as np
import pytest
from bson.objectid import ObjectId

from controller.binary_store import BinaryStore
from tests.conftest import fake_client, SECRET_KEY


def auth_headers(seeder, **kw):
    return {"project": str(seeder.project_id), **kw}


class TestAuth:
    def test_missing_project_header(self, client):
        r = client.get("/ds/datasets/", cookies={"jwt": "x"})
        assert r.status_code == 422

    def test_invalid_token(self, client, seeder):
        seeder.project()
        r = client.get("/ds/datasets/", headers=auth_headers(seeder),
                       cookies={"jwt": "garbage"})
        assert r.status_code == 401

    def test_expired_token(self, client, seeder):
        seeder.project()
        token = seeder.token(expired=True)
        r = client.get("/ds/datasets/", headers=auth_headers(seeder),
                       cookies={"jwt": token})
        assert r.status_code == 401

    def test_wrong_signature(self, client, seeder):
        seeder.project()
        token = seeder.token(key="other-key")
        r = client.get("/ds/datasets/", headers=auth_headers(seeder),
                       cookies={"jwt": token})
        assert r.status_code == 401

    def test_token_without_exp_rejected(self, client, seeder):
        seeder.project()
        token = seeder.token(no_exp=True)
        r = client.get("/ds/datasets/", headers=auth_headers(seeder),
                       cookies={"jwt": token})
        assert r.status_code == 401

    def test_unknown_project_rejected(self, client, seeder):
        # regression: used to crash with TypeError instead of 401
        r = client.get("/ds/datasets/",
                       headers={"project": str(ObjectId())},
                       cookies={"jwt": seeder.token()})
        assert r.status_code == 401

    def test_user_not_on_project(self, client, seeder):
        pid = seeder.project()
        outsider = ObjectId()
        r = client.get("/ds/datasets/", headers={"project": pid},
                       cookies={"jwt": seeder.token(user_id=outsider)})
        assert r.status_code == 401

    def test_member_user_allowed(self, client, seeder):
        member = str(ObjectId())
        pid = seeder.project(users=[member])
        r = client.get("/ds/datasets/", headers={"project": pid},
                       cookies={"jwt": seeder.token(user_id=ObjectId(member))})
        assert r.status_code == 200

    def test_api_key_wrong_access_type(self, client, seeder):
        seeder.project()
        key = seeder.api_key("rk", access="read")
        r = client.post(f"/ds/api/dataset/init/{key}?api_key={key}",
                        json={"name": "n", "timeSeries": ["a"]})
        assert r.status_code == 401

    def test_api_key_unknown(self, client, seeder):
        seeder.project()
        r = client.get("/ds/api/project/nokey?api_key=nokey")
        assert r.status_code == 401


class TestDatasetsRouter:
    def seed_dataset(self, ctrl=None, name="ds", n_points=5):
        from controller.dataset_controller import DatasetController
        seeder_obj = self._seeder
        from bson.objectid import ObjectId as _OID
        body = {
            "name": name,
            "timeSeries": [{"name": "a",
                            "data": [[i * 1000, float(i)] for i in range(n_points)]}],
            "userId": str(_OID()),
        }
        meta = DatasetController().addDataset(body, str(seeder_obj.project_id))
        return meta

    @pytest.fixture(autouse=True)
    def _seeded(self, seeder):
        self._seeder = seeder
        seeder.project()

    def test_get_datasets_metadata(self, client):
        self.seed_dataset()
        r = client.get("/ds/datasets/", headers=auth_headers(self._seeder),
                       cookies=self._seeder.cookies())
        assert r.status_code == 200
        assert len(r.json()) == 1

    def test_view_pagination(self, client):
        for n in ["b", "a", "c"]:
            self.seed_dataset(name=n)
        h = auth_headers(self._seeder)
        c = self._seeder.cookies()
        r = client.get("/ds/datasets/view", params={"skip": 0, "limit": 2, "sort": "alphaAsc"},
                       headers=h, cookies=c)
        assert [d["name"] for d in r.json()["datasets"]] == ["a", "b"]
        assert r.json()["total_datasets"] == 3
        r = client.get("/ds/datasets/view", params={"sort": "alphaDesc"},
                       headers=h, cookies=c)
        assert [d["name"] for d in r.json()["datasets"]] == ["c", "b", "a"]

    def test_view_invalid_params(self, client):
        h = auth_headers(self._seeder)
        r = client.get("/ds/datasets/view", params={"skip": -1}, headers=h,
                       cookies=self._seeder.cookies())
        assert r.status_code == 422

    def test_get_single(self, client):
        meta = self.seed_dataset()
        r = client.get(f"/ds/datasets/{meta['_id']}", headers=auth_headers(self._seeder),
                       cookies=self._seeder.cookies())
        assert r.status_code == 200

    def test_ts_endpoint_value_error_maps_to_400(self, client, monkeypatch):
        from routers import dataset as dataset_router
        meta = self.seed_dataset()

        def boom(*a, **k):
            raise ValueError("bad range")

        monkeypatch.setattr(dataset_router.ctrl, "getDataSetByIdStartEnd", boom)
        r = client.get(f"/ds/datasets/{meta['_id']}/ts/x/y/z",
                       headers=auth_headers(self._seeder), cookies=self._seeder.cookies())
        assert r.status_code == 400
        assert r.json() == {"message": "bad range"}

    def test_ts_endpoint_type_error_maps_to_400(self, client, monkeypatch):
        from routers import dataset as dataset_router
        meta = self.seed_dataset()

        def boom(*a, **k):
            raise TypeError("nope")

        monkeypatch.setattr(dataset_router.ctrl, "getDataSetByIdStartEnd", boom)
        r = client.get(f"/ds/datasets/{meta['_id']}/ts/x/y/z",
                       headers=auth_headers(self._seeder), cookies=self._seeder.cookies())
        assert r.status_code == 400
        assert r.json() == {"message": "Invalid input"}

    def test_create_dataset(self, client):
        body = {"name": "created", "timeSeries": [{"name": "t", "data": [[1, 2.0]]}]}
        r = client.post("/ds/datasets/", json=body, headers=auth_headers(self._seeder),
                        cookies=self._seeder.cookies())
        assert r.status_code == 200
        assert r.json()["name"] == "created"

    def test_update_and_delete_and_append(self, client):
        meta = self.seed_dataset()
        h = auth_headers(self._seeder)
        c = self._seeder.cookies()

        from utils.json_encoder import JSONEncoder
        full = fake_client["dataset_store_test"]["datasets"].find_one({})
        full_json = json.loads(json.dumps(full, cls=JSONEncoder))
        r = client.put(f"/ds/datasets/{meta['_id']}", json=full_json, headers=h, cookies=c)
        assert r.status_code == 200

        ts_id = str(meta["timeSeries"][0]["_id"])
        r = client.post(f"/ds/datasets/{meta['_id']}/append",
                        json=[{"_id": ts_id, "data": [[9000, 9.9]]}],
                        headers=h, cookies=c)
        assert r.status_code == 200
        # error inside append is swallowed -> still 200
        r = client.post(f"/ds/datasets/{meta['_id']}/append", json=[{"bogus": 1}],
                        headers=h, cookies=c)
        assert r.status_code == 200

        r = client.delete(f"/ds/datasets/{meta['_id']}", headers=h, cookies=c)
        assert r.status_code == 200

    def test_create_with_csv(self, client):
        csv = b"time,sensor_x\n100.5,1.0\n"
        config = {
            "name": "csv-created",
            "timeSeries": [{"originalName": "x", "originalUnit": "", "name": "x",
                            "unit": "", "removed": False, "scale": 1.0, "offset": 0.0}],
            "labelings": [],
        }
        r = client.post(
            "/ds/datasets/create",
            files={"CSVFile": ("data.csv", io.BytesIO(csv), "text/csv")},
            data={"CSVConfig": __import__("json").dumps(config)},
            headers=auth_headers(self._seeder), cookies=self._seeder.cookies())
        assert r.status_code == 200
        assert r.json()["name"] == "csv-created"

    def test_create_with_csv_error_maps_to_400(self, client, monkeypatch):
        from routers import dataset as dataset_router

        def boom(*a, **k):
            raise RuntimeError("broken csv")

        monkeypatch.setattr(dataset_router.ctrl, "CSVUpload", boom)
        r = client.post(
            "/ds/datasets/create",
            files={"CSVFile": ("data.csv", io.BytesIO(b"a,b\n"), "text/csv")},
            data={"CSVConfig": "{}"},
            headers=auth_headers(self._seeder), cookies=self._seeder.cookies())
        assert r.status_code == 400
        assert "broken csv" in r.json()["detail"]

    def test_timeseries_partially(self, client, monkeypatch):
        from routers import dataset as dataset_router
        meta = self.seed_dataset()

        arr = np.array([[0.0, 1.0], [1.0, 2.0]])
        monkeypatch.setattr(dataset_router.ctrl, "getDataSetByIdStartEnd",
                            lambda *a, **k: [arr])
        r = client.get(f"/ds/datasets/{meta['_id']}/ts/undefined/undefined/50",
                       headers=auth_headers(self._seeder), cookies=self._seeder.cookies())
        assert r.status_code == 200

    def test_single_timeseries_partially(self, client, monkeypatch):
        from routers import dataset as dataset_router
        meta = self.seed_dataset()
        ts_id = str(meta["timeSeries"][0]["_id"])

        arr = np.array([[0.0, 1.0]])
        monkeypatch.setattr(dataset_router.ctrl, "getDatasetTimeSeriesStartEnd",
                            lambda *a, **k: arr)
        r = client.get(f"/ds/datasets/{meta['_id']}/ts/{ts_id}/undefined/undefined/10",
                       headers=auth_headers(self._seeder), cookies=self._seeder.cookies())
        assert r.status_code == 200


class TestLabelingsRouter:
    @pytest.fixture(autouse=True)
    def _seeded(self, seeder):
        seeder.project()

    def test_crud(self, client, seeder):
        h = auth_headers(seeder)
        c = seeder.cookies()
        base = "/ds/datasets/labelings" if False else "/ds/labelings"

        r = client.get(base + "/", headers=h, cookies=c)
        assert r.status_code == 200 and r.json() == []

        r = client.post(base + "/", json={"name": "L", "labels": []}, headers=h, cookies=c)
        assert r.status_code == 200
        labeling_id = r.json()["_id"]

        r = client.put(f"{base}/{labeling_id}",
                       json={"_id": str(labeling_id), "projectId": str(seeder.project_id),
                             "name": "L", "labels": [{"name": "a", "color": "#000000"}]},
                       headers=h, cookies=c)
        assert r.status_code == 200

        r = client.delete(f"{base}/{labeling_id}", headers=h, cookies=c)
        assert r.status_code == 200


class TestDatasetLabelsRouter:
    @pytest.fixture(autouse=True)
    def _seeded(self, seeder):
        seeder.project()

    def _make_labeling_and_dataset(self, client, seeder):
        h = auth_headers(seeder)
        c = seeder.cookies()
        r = client.post("/ds/labelings/", json={"name": "L", "labels": [
            {"name": "run", "color": "#123456"}]}, headers=h, cookies=c)
        labeling = r.json()
        ds_body = {"name": "labeled", "timeSeries": [{"name": "a", "data": [[1, 1.0]]}]}
        r = client.post("/ds/datasets/", json=ds_body, headers=h, cookies=c)
        dataset_id = r.json()["_id"]
        return labeling, dataset_id, h, c

    def test_create_update_delete_label(self, client, seeder):
        labeling, dataset_id, h, c = self._make_labeling_and_dataset(client, seeder)
        type_id = str(labeling["labels"][0]["_id"])
        labeling_id = str(labeling["_id"])
        base = f"/ds/datasets/labelings"

        r = client.post(f"{base}/{dataset_id}/{labeling_id}",
                        json={"start": 1, "end": 5, "type": type_id},
                        headers=h, cookies=c)
        assert r.status_code == 200
        label_id = r.json()["_id"]

        r = client.put(f"{base}/{dataset_id}/{labeling_id}/{label_id}",
                       json={"start": 2, "end": 6, "type": type_id, "_id": label_id},
                       headers=h, cookies=c)
        assert r.status_code == 200

        r = client.delete(f"{base}/{dataset_id}/{labeling_id}/{label_id}",
                          headers=h, cookies=c)
        assert r.status_code == 200


class TestDownloadRouter:
    def _seed_ready(self, seeder):
        from controller.dataset_controller import DatasetController
        seeder.project()
        from bson.objectid import ObjectId as _OID
        body = {"name": "dl-ds", "userId": str(_OID()),
                "timeSeries": [{"name": "s", "data": [[0, 1.0], [1, 2.0]]}]}
        meta = DatasetController().addDataset(body, str(seeder.project_id))
        return meta

    def test_full_flow(self, client, seeder):
        meta = self._seed_ready(seeder)
        h = auth_headers(seeder)
        c = seeder.cookies()

        r = client.post(f"/ds/download/dataset/{meta['_id']}", headers=h, cookies=c)
        assert r.status_code == 200
        download_id = r.json()["downloadId"]

        r = client.get("/ds/download/status/", headers=h, cookies=c)
        assert r.status_code == 200
        entry = next(x for x in r.json() if x["downloadId"] == download_id)
        assert entry["status"] == 100

        r = client.get(f"/ds/download/{download_id}")
        assert r.status_code == 200
        assert "attachment" in r.headers["content-disposition"]

        r = client.delete(f"/ds/download/{download_id}", headers=h, cookies=c)
        assert r.status_code == 200

    def test_download_not_ready_returns_409(self, client, seeder):
        from db.csv import csvDB, DBEntryDataset
        seeder.project()
        csvDB().add(DBEntryDataset(downloadId="pending-1", projectId=seeder.project_id,
                                   userId=ObjectId(), projectName="p", datasetName="d"))
        r = client.get("/ds/download/pending-1")
        assert r.status_code == 409

    def test_register_project(self, client, seeder):
        self._seed_ready(seeder)
        h = dict(auth_headers(seeder))
        h["project"] = str(seeder.project_id)
        r = client.post("/ds/download/project", headers=h, cookies=seeder.cookies())
        assert r.status_code == 200
