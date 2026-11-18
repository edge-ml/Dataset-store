"""Adding the data of a CSV file to an existing dataset (#95)."""
import io
import json

from bson.objectid import ObjectId

from controller.binary_store import BinaryStore

CSV = (
    "time,sensor_accX[g],sensor_temp,label_activity_walk,label_activity_run\n"
    "1000,1.0,20,x,\n"
    "1020,2.0,21,x,\n"
    "1040,3.0,22,,x\n"
    "1060,4.0,23,,x\n"
)


def config(**kw):
    return {
        "name": kw.get("name", "extra"),
        "timeSeries": [
            {"originalName": "accX", "originalUnit": "g", "name": "accX", "unit": "g", "quantity": "acceleration",
             "removed": False, "scale": 1.0, "offset": 0.0},
            {"originalName": "temp", "originalUnit": "", "name": "temp", "unit": "", "removed": kw.get("drop_temp", False),
             "scale": 1.0, "offset": 0.0},
        ],
        "labelings": [{"originalName": "activity", "name": "activity", "removed": False, "labels": ["walk", "run"]}],
    }


def post(client, seeder, dataset_id, cfg=None, body=CSV):
    return client.post(
        f"/ds/datasets/{dataset_id}/csv",
        files={"CSVFile": ("extra.csv", io.BytesIO(body.encode()), "text/csv")},
        data={"CSVConfig": json.dumps(cfg or config())},
        headers=seeder.headers(), cookies=seeder.cookies())


def test_adds_series_and_labels_to_a_dataset_with_only_recordings(client, seeder):
    seeder.project()
    ds = seeder.dataset(time_series=[])
    seeder.datasets.update_one({"_id": ds["_id"]}, {"$set": {"mediaSeries": [{"_id": ObjectId(), "kind": "audio", "name": "mic",
                                                                                "start": 900, "end": 2000, "durationMs": 1100,
                                                                                "mimeType": "audio/wav", "size": 1}]}})
    r = post(client, seeder, ds["_id"])
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["added"] == ["accX", "temp"] and body["skippedLabels"] == 0
    doc = seeder.datasets.find_one({"_id": ds["_id"]})
    assert [t["name"] for t in doc["timeSeries"]] == ["accX", "temp"]
    acc = doc["timeSeries"][0]
    assert (acc["start"], acc["end"], acc["length"], acc["unit"], acc["quantity"]) == (1000, 1060, 4, "g", "acceleration")
    BinaryStore_ = BinaryStore(acc["_id"])
    BinaryStore_.loadSeries()
    assert BinaryStore_.getFull()["data"].tolist() == [1.0, 2.0, 3.0, 4.0]
    # the recording is still there, the labels arrived
    assert [m["name"] for m in doc["mediaSeries"]] == ["mic"]
    labels = doc["labelings"][0]["labels"]
    assert sorted((l["start"], l["end"]) for l in labels) == [(1000, 1020), (1040, 1060)]


def test_names_already_taken_get_a_number(client, seeder):
    seeder.project()
    ds = seeder.dataset(time_series=[{"name": "accX", "start": 0, "end": 10}])
    assert post(client, seeder, ds["_id"], config(drop_temp=True)).json()["added"] == ["accX (2)"]
    assert post(client, seeder, ds["_id"], config(drop_temp=True)).json()["added"] == ["accX (3)"]


def test_overlapping_labels_are_left_out(client, seeder):
    seeder.project()
    ds = seeder.dataset(time_series=[])
    assert post(client, seeder, ds["_id"]).json()["skippedLabels"] == 0
    # the same file again: its labels lie exactly on the ones already there
    again = post(client, seeder, ds["_id"]).json()
    assert again["skippedLabels"] == 2
    doc = seeder.datasets.find_one({"_id": ds["_id"]})
    assert len(doc["labelings"]) == 1 and len(doc["labelings"][0]["labels"]) == 2
    assert len(doc["timeSeries"]) == 4


def test_errors(client, seeder):
    seeder.project()
    ds = seeder.dataset(time_series=[])
    assert post(client, seeder, ObjectId()).status_code == 404
    r = client.post(f"/ds/datasets/{ds['_id']}/csv", files={"CSVFile": ("x.csv", io.BytesIO(b"x"), "text/csv")},
                    data={"CSVConfig": "{"}, headers=seeder.headers(), cookies=seeder.cookies())
    assert r.status_code == 400
    assert post(client, seeder, ds["_id"], body="time,sensor_accX[g],sensor_temp\n").status_code == 400
    # nothing half-added
    assert seeder.datasets.find_one({"_id": ds["_id"]})["timeSeries"] == []
    r = client.post(f"/ds/datasets/{ds['_id']}/csv", files={"CSVFile": ("x.csv", io.BytesIO(CSV.encode()), "text/csv")},
                    data={"CSVConfig": json.dumps(config())}, headers=seeder.headers(), cookies={"jwt": "nope"})
    assert r.status_code == 401
