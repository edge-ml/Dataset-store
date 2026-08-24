import datetime

import pytest
from bson.objectid import ObjectId

from db.async_device_upload import AsyncUploadDB, UploadRequest
from db.csv import csvDB, DBEntryDataset, DBEntryProject
from db.dataset import DatasetDBManager, DatasetLabel, TimeSeries
from db.deviceAPi import DeviceApiManager
from db.labelings import LabelingDBManager
from db.project import ProjectDBManager
from db.timeseries import TimeseriesDBManager


@pytest.fixture
def dbm():
    return DatasetDBManager()


def make_dataset_doc(name="ds", project_id=None):
    return {
        "name": name,
        "projectId": project_id or ObjectId(),
        "userId": ObjectId(),
        "metaData": {"a": "b"},
        "timeSeries": [{"name": "ts1"}],
        "labelings": [],
    }


class TestDatasetDBManager:
    def test_add_and_get_by_id(self, dbm):
        doc = make_dataset_doc()
        pid = doc["projectId"]
        inserted = dbm.addDataset(doc)
        assert "_id" in inserted
        got = dbm.getDatasetById(str(inserted["_id"]), str(pid))
        assert got["name"] == "ds"

    def test_add_invalid_raises(self, dbm):
        # missing required fields -> pydantic ValidationError
        with pytest.raises(Exception):
            dbm.addDataset({"timeSeries": [{"name": "x"}]})

    def test_delete_returns_timeseries_ids(self, dbm):
        doc = dbm.addDataset(make_dataset_doc())
        ts_ids = dbm.deleteDatasetById(doc["projectId"], doc["_id"])
        assert len(ts_ids) == 1
        assert dbm.getDatasetById(doc["_id"], doc["projectId"]) is None

    def test_get_datasets_in_project(self, dbm):
        doc = dbm.addDataset(make_dataset_doc())
        datasets = list(dbm.getDatasetsInProjet(doc["projectId"]))
        assert len(datasets) == 1

    @pytest.mark.parametrize("sort", ["alphaAsc", "alphaDesc", "dateAsc", "dateDesc"])
    def test_pagination_sorts(self, dbm, sort):
        doc_a = dbm.addDataset(make_dataset_doc(name="a"))
        dbm.addDataset(make_dataset_doc(name="b", project_id=doc_a["projectId"]))
        pid = doc_a["projectId"]
        datasets, count = dbm.getDatasetsInProjetPagination(pid, 0, 10, sort, None)
        names = [d["name"] for d in datasets]
        if sort.startswith("alpha"):
            expected = ["a", "b"] if sort.endswith("Asc") else ["b", "a"]
            assert names == expected
        assert count == 2

    def test_pagination_skip_limit(self, dbm):
        doc = dbm.addDataset(make_dataset_doc())
        for i in range(5):
            dbm.addDataset(make_dataset_doc(name=f"ds{i}", project_id=doc["projectId"]))
        page, count = dbm.getDatasetsInProjetPagination(doc["projectId"], 2, 2, "alphaAsc", None)
        assert len(list(page)) == 2
        assert count == 6

    def test_update(self, dbm):
        doc = dbm.addDataset(make_dataset_doc())
        doc["name"] = "renamed"
        updated = dbm.updateDataset(doc["_id"], doc["projectId"], doc)
        assert updated["name"] == "renamed"
        assert dbm.getDatasetById(doc["_id"], doc["projectId"])["name"] == "renamed"

    def test_partial_update(self, dbm):
        doc = dbm.addDataset(make_dataset_doc())
        dbm.partialUpdate(doc["_id"], doc["projectId"], {"name": "part"})
        assert dbm.getDatasetById(doc["_id"], doc["projectId"])["name"] == "part"

    def test_update_time_series_unit(self):
        dbm = DatasetDBManager()
        ts_id = ObjectId()
        doc = make_dataset_doc()
        doc["timeSeries"] = [{"_id": ts_id, "name": "a", "unit": ""}]
        added = dbm.addDataset(doc)
        dbm.updateTimeSeriesUnit(added["_id"], ts_id, added["projectId"], "m/s")
        got = dbm.getDatasetById(added["_id"], added["projectId"])
        assert got["timeSeries"][0]["unit"] == "m/s"

    def test_update_time_series_unit_config(self):
        dbm = DatasetDBManager()
        ts_id = ObjectId()
        doc = make_dataset_doc()
        doc["timeSeries"] = [{"_id": ts_id, "name": "a", "unit": ""}]
        added = dbm.addDataset(doc)
        dbm.updateTimeSeriesUnitConfig(added["_id"], ts_id, added["projectId"], "g", 2.0, -1.5)
        ts = dbm.getDatasetById(added["_id"], added["projectId"])["timeSeries"][0]
        assert ts["unit"] == "g"
        assert ts["scaling"] == 2.0
        assert ts["offset"] == -1.5

    def test_delete_project(self, dbm):
        # regression: used to delete by _id instead of projectId (deleted nothing)
        doc = dbm.addDataset(make_dataset_doc())
        dbm.deleteProject(doc["projectId"])
        assert dbm.getDatasetById(doc["_id"], doc["projectId"]) is None

    def test_update_dataset_label(self, dbm):
        labeling_id = ObjectId()
        label_id = ObjectId()
        new_label = {"start": 5, "end": 10}
        doc = make_dataset_doc()
        doc["labelings"] = [{"labelingId": labeling_id,
                             "labels": [{"_id": label_id, "type": ObjectId(),
                                         "start": 0, "end": 3}]}]
        added = dbm.addDataset(doc)
        dbm.updateDatasetLabel(added["projectId"], added["_id"], labeling_id, label_id, new_label)
        updated = dbm.getDatasetById(added["_id"], added["projectId"])
        assert updated["labelings"][0]["labels"][0]["end"] == 10

    def test_models(self):
        ts = TimeSeries(name="x")
        assert ts.id is not None
        with pytest.raises(ValueError):
            DatasetLabel(start=5, end=4)  # end before start rejected
        ok = DatasetLabel(start=5, end=5)  # point labels allowed
        assert ok.end == 5


class TestCSVDB:
    def test_add_get_update_delete(self):
        db = csvDB()
        entry = DBEntryDataset(downloadId="abc", projectId=ObjectId(),
                               userId=ObjectId(), projectName="p", datasetName="d")
        db.add(entry)
        got = db.get("abc")
        assert isinstance(got, DBEntryDataset)
        db.update(download_id="abc", status=100, fileName="f.csv", filePath="/tmp/f.csv")
        assert db.get("abc").status == 100
        assert db.get("abc").fileName == "f.csv"
        assert len(db.get_by_user(entry.userId)) == 1
        entries = db.getOlder(60 * 60)  # created just now: not old after 1h threshold
        assert all(e.downloadId != "abc" for e in entries)
        db.delete("abc")
        with pytest.raises(Exception):
            db.get("abc")

    def test_project_entry(self):
        db = csvDB()
        entry = DBEntryProject(downloadId="xyz", projectId=ObjectId(),
                               userId=ObjectId(), projectName="p")
        db.add(entry)
        assert isinstance(db.get("xyz"), DBEntryProject)

    def test_get_older(self):
        db = csvDB()
        entry = DBEntryDataset(downloadId="old", projectId=ObjectId(),
                               userId=ObjectId(), projectName="p", datasetName="d")
        db.add(entry)
        db.col.update_one({"downloadId": "old"},
                          {"$set": {"created_at": datetime.datetime.utcnow() - datetime.timedelta(hours=2)}})
        old_entries = db.getOlder(3600)
        assert any(e.downloadId == "old" for e in old_entries)


class TestProjectDBManager:
    def test_get_project(self, seeder):
        pid = seeder.project()
        dbm = ProjectDBManager()
        project = dbm.get_project(pid)
        assert project["name"] == "test-project"

    def test_missing_project(self):
        dbm = ProjectDBManager()
        assert dbm.get_project(ObjectId()) is None


class TestDeviceApiManager:
    def test_write_key(self, seeder):
        key = seeder.api_key("wkey", access="write")
        res = DeviceApiManager().get(key)
        assert res["access_type"] == "write"
        assert res["projectId"] == seeder.project_id

    def test_read_key(self, seeder):
        key = seeder.api_key("rkey", access="read")
        res = DeviceApiManager().get(key)
        assert res["access_type"] == "read"

    def test_unknown_key(self):
        assert DeviceApiManager().get("nope") is None


class TestLabelingDBManager:
    def test_create_and_merge_labels(self):
        dbm = LabelingDBManager()
        pid = str(ObjectId())
        created = dbm.create(pid, {"name": "L", "projectId": ObjectId(pid),
                                    "labels": [{"name": "a", "color": "#000000"}]})
        assert len(created["labels"]) == 1
        merged = dbm.create(pid, {"name": "L", "projectId": ObjectId(pid),
                                  "labels": [{"name": "a", "color": "#111111"},
                                             {"name": "b", "color": "#222222"}]})
        assert len(merged["labels"]) == 2  # duplicate 'a' not re-added

    def test_get_single_and_get(self):
        dbm = LabelingDBManager()
        pid = str(ObjectId())
        created = dbm.create(pid, {"name": "L", "projectId": ObjectId(pid), "labels": []})
        assert dbm.get_single(pid, created["_id"])["name"] == "L"
        assert len(dbm.get(pid)) == 1
        assert dbm.get(str(ObjectId())) == []

    def test_update_and_delete(self):
        dbm = LabelingDBManager()
        pid = str(ObjectId())
        created = dbm.create(pid, {"name": "L", "projectId": ObjectId(pid), "labels": []})
        updated = dbm.update(pid, created["_id"], {
            "_id": created["_id"], "projectId": ObjectId(pid),
            "name": "L", "labels": [{"name": "x", "color": "#123456"}]})
        assert updated["labels"][0]["name"] == "x"
        dbm.delete(pid, created["_id"])
        assert dbm.get_single(pid, created["_id"]) is None

    def test_delete_project(self):
        # regression: queried 'project_id' but documents store 'projectId'
        dbm = LabelingDBManager()
        pid = str(ObjectId())
        dbm.create(pid, {"name": "L", "projectId": ObjectId(pid), "labels": []})
        dbm.deleteProject(pid)
        assert dbm.get(pid) == []


class TestTimeseriesDBManager:
    def test_crud(self):
        dbm = TimeseriesDBManager()
        oid = str(ObjectId())
        dbm.addTimeSeries({"_id": oid, "name": "t", "start": 1, "end": 2})
        assert dbm.gettimeSeriesById(oid)["name"] == "t"
        assert len(list(dbm.getTimeSeries(oid))) >= 0
        dbm.updateStartEnd(oid, 10, 20)
        assert dbm.gettimeSeriesById(oid)["start"] == 10
        res = dbm.deleteTimeSeries(oid)
        assert res.deleted_count == 1


class TestAsyncUploadDB:
    def test_flow(self):
        db = AsyncUploadDB()
        uid = ObjectId()
        req = UploadRequest(_id=str(ObjectId()), user_id=uid)
        db.add_upload_request(req)
        db.setError(req.id, "boom")
        status = db.getStatus(req.id, uid)
        assert status.error == "boom"
        assert status.status == 0
        db.setStatus_finished(req.id)
        assert db.getStatus(req.id, uid).status == 100
