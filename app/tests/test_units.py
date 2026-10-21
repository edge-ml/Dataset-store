"""Quantity and unit per time-series: setting/correcting them, and recording
them on device-API and CSV uploads. Series without a quantity stay unknown."""
import copy
from io import BytesIO

import pytest
from bson.objectid import ObjectId

from controller.dataset_controller import DatasetController
from tests.conftest import fake_client, DATASTORE_DBNAME, DATASTORE_COLLNAME

CSV_CONTENT = (
    b"time,sensor_speed[m/s],label_Activity_run\n"
    b"100.500,1.0,x\n"
    b"101.600,2.0,x\n"
    b"102.700,3.0,\n"
)
CSV_CONFIG = {
    "name": "",
    "timeSeries": [
        {"originalName": "speed", "originalUnit": "m/s", "name": "speed",
         "unit": "m/s", "removed": False, "scale": 1.0, "offset": 0.0},
    ],
    "labelings": [
        {"originalName": "Activity", "name": "Activity", "removed": False, "labels": ["run"]},
    ],
}


@pytest.fixture
def ctrl():
    return DatasetController()


def upload(content):
    class F:
        filename = "upload.csv"
        file = BytesIO(content)
    return F()


def stored_series():
    return fake_client[DATASTORE_DBNAME][DATASTORE_COLLNAME].find_one({})["timeSeries"]


class TestSetUnit:
    def url(self, ds, ts_id):
        return f"/ds/datasets/{ds['_id']}/ts/{ts_id}/unit"

    def test_set_and_correct(self, client, seeder):
        pid = seeder.project()
        ds = seeder.dataset([{"name": "accX", "unit": "m/s^2", "start": 1000, "end": 2000}])
        ts_id = ds["timeSeries"][0]["_id"]
        kw = dict(headers={"project": pid}, cookies={"jwt": seeder.token()})

        r = client.put(self.url(ds, ts_id), json={"quantity": "acceleration", "unit": "g"}, **kw)
        assert r.status_code == 200
        assert r.json() == {"quantity": "acceleration", "unit": "g"}
        assert stored_series()[0]["quantity"] == "acceleration"
        assert stored_series()[0]["unit"] == "g"

        # Back to unknown: empty quantity is stored as None.
        r = client.put(self.url(ds, ts_id), json={"quantity": " ", "unit": "raw"}, **kw)
        assert r.json() == {"quantity": None, "unit": "raw"}
        assert stored_series()[0]["quantity"] is None

    def test_unknown_series_is_404(self, client, seeder):
        pid = seeder.project()
        ds = seeder.dataset()
        r = client.put(self.url(ds, ObjectId()), json={"quantity": "angle", "unit": "deg"},
                       headers={"project": pid}, cookies={"jwt": seeder.token()})
        assert r.status_code == 404

    def test_requires_login(self, client, seeder):
        pid = seeder.project()
        ds = seeder.dataset()
        r = client.put(self.url(ds, ds["timeSeries"][0]["_id"]), json={"unit": "g"},
                       headers={"project": pid}, cookies={"jwt": "garbage"})
        assert r.status_code == 401

    def test_dataset_reads_include_quantity(self, client, seeder):
        pid = seeder.project()
        ds = seeder.dataset([{"name": "t", "unit": "degC", "quantity": "temperature",
                              "start": 1000, "end": 2000}, {"name": "old", "start": 1000, "end": 2000}])
        r = client.get(f"/ds/datasets/{ds['_id']}", headers={"project": pid},
                       cookies={"jwt": seeder.token()})
        series = {s["name"]: s for s in r.json()["timeSeries"]}
        assert series["t"]["quantity"] == "temperature"
        assert series["old"].get("quantity") is None


class TestDeviceInit:
    def test_names_and_objects(self, client, seeder):
        seeder.project()
        key = seeder.api_key("w-key", access="write")
        r = client.post(f"/ds/api/dataset/init/{key}?api_key={key}", json={
            "name": "n",
            "timeSeries": ["plain", {"name": "acc", "unit": "g", "quantity": "acceleration"},
                           {"name": "strain", "unit": "ue"}],
        })
        assert r.status_code == 200
        series = {s["name"]: s for s in stored_series()}
        assert series["plain"].get("quantity") is None and series["plain"].get("unit", "") == ""
        assert (series["acc"]["quantity"], series["acc"]["unit"]) == ("acceleration", "g")
        assert (series["strain"]["quantity"], series["strain"]["unit"]) == (None, "ue")


class TestCsvQuantity:
    def test_quantity_from_upload_config(self, ctrl, seeder):
        pid = seeder.project()
        config = copy.deepcopy(CSV_CONFIG)
        config["timeSeries"][0]["quantity"] = "velocity"
        ctrl.CSVUpload(upload(CSV_CONTENT), config, pid, str(ObjectId()))
        assert stored_series()[0]["quantity"] == "velocity"

    def test_no_quantity_stays_unknown(self, ctrl, seeder):
        pid = seeder.project()
        ctrl.CSVUpload(upload(CSV_CONTENT), copy.deepcopy(CSV_CONFIG), pid, str(ObjectId()))
        assert stored_series()[0]["quantity"] is None
