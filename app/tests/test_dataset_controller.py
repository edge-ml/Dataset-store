import json
from io import BytesIO

import numpy as np
import pytest
from bson.objectid import ObjectId
from fastapi import HTTPException

from controller.dataset_controller import DatasetController
from controller.binary_store import BinaryStore


@pytest.fixture
def ctrl():
    return DatasetController()


def dataset_body(name="ds", data=None):
    return {
        "name": name,
        "userId": str(ObjectId()),
        "timeSeries": [{
            "name": "a",
            "data": data if data is not None else [[1000, 1.0], [2000, 2.0], [3000, 3.5]],
        }],
    }


class TestAddDataset:
    def test_success(self, ctrl, seeder):
        pid = seeder.project()
        res = ctrl.addDataset(dataset_body(), pid, user_id=str(ObjectId()))
        assert res["_id"]
        stored = seeder.datasets.find_one({})
        assert stored["timeSeries"][0]["start"] == 1000
        assert stored["timeSeries"][0]["length"] == 3

    def test_without_user_id(self, ctrl, seeder):
        # regression: used to raise NameError because newDatasetMeta was unbound
        pid = seeder.project()
        res = ctrl.addDataset(dataset_body(), pid, user_id=None)
        assert res["_id"] is not None

    def test_failure_rolls_back(self, ctrl, seeder):
        pid = seeder.project()
        with pytest.raises(Exception):
            ctrl.addDataset(dataset_body(data=[["bad", "worse"]]), pid, user_id=str(ObjectId()))
        assert seeder.datasets.count_documents({}) == 0  # dataset deleted again


class TestGetDataset:
    def test_only_meta(self, ctrl, seeder):
        pid = seeder.project()
        meta = ctrl.addDataset(dataset_body(), pid)
        got = ctrl.getDatasetById(meta["_id"], pid, onlyMeta=True)
        assert "data" not in got["timeSeries"][0]

    def test_full_loads_series(self, ctrl, seeder):
        pid = seeder.project()
        meta = ctrl.addDataset(dataset_body(), pid)
        got = ctrl.getDatasetById(meta["_id"], pid, onlyMeta=False)
        ts_data = got["timeSeries"][0]["data"]
        assert [d[0] for d in ts_data] == [1000.0, 2000.0, 3000.0]


class TestProjectQueries:
    def test_get_in_project_no_labelings(self, ctrl, seeder):
        pid = seeder.project()
        ctrl.addDataset(dataset_body(), pid)
        datasets = ctrl.getDatasetInProject(pid)
        assert datasets[0]["labelings"] == []

    def test_populate_labelings(self, ctrl, seeder):
        # regression: _populateLabelings iterated an undefined variable 'ds'
        pid = seeder.project()
        label_id = ObjectId()
        labeling_id = ObjectId()
        seeder.labelings.insert_one({
            "_id": labeling_id, "projectId": seeder.project_id, "name": "L",
            "labels": [{"_id": label_id, "name": "run", "color": "#000000"}]})
        meta = ctrl.addDataset(dataset_body(), pid)
        doc = seeder.datasets.find_one({})
        doc["labelings"] = [{"labelingId": labeling_id,
                             "labels": [{"type": label_id, "start": 1, "end": 2}]}]
        seeder.datasets.replace_one({"_id": doc["_id"]}, doc)

        datasets = ctrl.getDatasetInProject(pid, includeTimeseriesData=True)
        assert datasets[0]["labelings"][0]["labeling"] == "L"
        assert datasets[0]["labelings"][0]["labels"][0]["label"] == "run"

    def test_pagination(self, ctrl, seeder):
        pid = seeder.project()
        for i in range(4):
            ctrl.addDataset(dataset_body(name=f"d{i}"), pid)
        datasets, count = ctrl.getDatasetInProjectWithPagination(pid, 0, 2, "alphaAsc", None)
        assert len(datasets) == 2 and count == 4
        datasets, count = ctrl.getDatasetInProjectWithPagination(pid, 0, 2, "alphaAsc", None,
                                                                 includeTimeseriesData=True)
        assert "labelings" in datasets[0]


class TestTimeRanges:
    def test_start_end(self, ctrl, seeder):
        pid = seeder.project()
        meta = ctrl.addDataset(dataset_body(), pid)
        res = ctrl.getDataSetByIdStartEnd(meta["_id"], pid, "undefined", "undefined", "50")
        assert len(res) == 1
        # small series (<200 points) are returned unfiltered by design
        res2 = ctrl.getDataSetByIdStartEnd(meta["_id"], pid, "1500", "2500", "100000")
        assert res2[0].shape == (3, 2)

    def test_single_timeseries_range(self, ctrl, seeder):
        pid = seeder.project()
        meta = ctrl.addDataset(dataset_body(), pid)
        ts_id = str(meta["timeSeries"][0]["_id"])
        res = ctrl.getDatasetTimeSeriesStartEnd(meta["_id"], ts_id, pid, "undefined", "undefined", "10")
        assert res.shape == (3, 2)

    def test_single_timeseries_wrong_id(self, ctrl, seeder):
        pid = seeder.project()
        meta = ctrl.addDataset(dataset_body(), pid)
        with pytest.raises(HTTPException) as e:
            ctrl.getDatasetTimeSeriesStartEnd(meta["_id"], str(ObjectId()), pid,
                                              "undefined", "undefined", "10")
        assert e.value.status_code == 404


class TestUpdateDelete:
    def test_update(self, ctrl, seeder):
        pid = seeder.project()
        meta = ctrl.addDataset(dataset_body(), pid)
        meta["name"] = "new"
        updated = ctrl.updateDataset(meta["_id"], pid, meta)
        assert updated["name"] == "new"

    def test_delete(self, ctrl, seeder):
        from internal.config import TSDATA
        import os
        pid = seeder.project()
        meta = ctrl.addDataset(dataset_body(), pid)
        path = os.path.join(TSDATA, str(meta["timeSeries"][0]["_id"]) + ".bin")
        assert os.path.exists(path)
        ctrl.deleteDataset(meta["_id"], pid)
        assert not os.path.exists(path)
        assert seeder.datasets.count_documents({}) == 0

    def test_delete_project_datasets(self, ctrl, seeder):
        from internal.config import TSDATA
        import os
        pid = seeder.project()
        meta = ctrl.addDataset(dataset_body(), pid)
        ctrl.deleteProjectDatasets(pid)
        assert seeder.datasets.count_documents({}) == 0
        assert not os.path.exists(os.path.join(TSDATA, str(meta["timeSeries"][0]["_id"]) + ".bin"))

    def test_get_time_series_data_hdf5(self, ctrl, seeder):
        import h5py
        pid = seeder.project()
        meta = ctrl.addDataset(dataset_body(), pid)
        path = ctrl.getTimeSeriesData(pid, meta["_id"], str(meta["timeSeries"][0]["_id"]))
        try:
            with h5py.File(path, "r") as f:
                assert f["time"].shape == (3,)
        finally:
            import os
            os.remove(path)


class TestAppend:
    def make_dataset_with_ts_ids(self, ctrl, seeder):
        pid = seeder.project()
        meta = ctrl.addDataset(dataset_body(), pid)
        return meta, pid

    def test_append_mismatched_ids(self, ctrl, seeder):
        meta, pid = self.make_dataset_with_ts_ids(ctrl, seeder)
        with pytest.raises(HTTPException) as e:
            ctrl.append(meta["_id"], pid, [{"_id": str(ObjectId()), "data": [[4000, 1]]}],
                        projectId=pid)
        assert e.value.status_code == 401

    def test_append_extends_range(self, ctrl, seeder):
        meta, pid = self.make_dataset_with_ts_ids(ctrl, seeder)
        ts_id = str(meta["timeSeries"][0]["_id"])
        ctrl.append(meta["_id"], pid, [{"_id": ts_id, "data": [[2500, 9], [5000, 1]]}],
                    projectId=pid)
        doc = ctrl.dbm.getDatasetById(meta["_id"], pid)
        ts = doc["timeSeries"][0]
        assert ts["start"] == 1000 and ts["end"] == 5000

    def test_append_when_old_bounds_missing(self, ctrl, seeder):
        meta, pid = self.make_dataset_with_ts_ids(ctrl, seeder)
        ts_id = str(meta["timeSeries"][0]["_id"])
        doc = ctrl.dbm.getDatasetById(meta["_id"], pid)
        doc["timeSeries"][0]["start"] = None
        doc["timeSeries"][0]["end"] = None
        ctrl.dbm.updateDataset(meta["_id"], pid, doc)
        ctrl.append(meta["_id"], pid, [{"_id": ts_id, "data": [[500, 9]]}], projectId=pid)
        doc = ctrl.dbm.getDatasetById(meta["_id"], pid)
        assert doc["timeSeries"][0]["start"] == 500


class TestExternalUpload:
    def test_external_upload(self, ctrl, seeder):
        pid = seeder.project()
        key = seeder.api_key("k1", access="write")
        body = {
            "name": "ext",
            "labeling": {"name": "L", "labels": [{"name": "pos", "color": "", "start": 10, "end": 20}]},
            "timeSeries": [{"name": "a", "data": [[10, 1.0], [20, 2.0]]}],
        }
        ds_id = ctrl.externalUpload(key, str(ObjectId()), body)
        assert seeder.datasets.find_one({"_id": ObjectId(ds_id)}) is not None
        labeling = seeder.labelings.find_one({})
        assert labeling["name"] == "L"


class TestCSVUpload:
    def upload_file(self, content: bytes):
        class F:
            filename = "upload.csv"
            file = BytesIO(content)
        return F()

    CSV_CONTENT = (
        b"time,sensor_temp[m/s],label_Activity_run\n"
        b"100.500,1.0,x\n"
        b"101.600,2.0,x\n"
        b"102.700,3.0,\n"
    )

    CONFIG = {
        "name": "",
        "timeSeries": [
            {"originalName": "temp", "originalUnit": "m/s", "name": "temp_new",
             "unit": "m/s", "removed": False, "scale": 1.0, "offset": 0.0},
        ],
        "labelings": [
            {"originalName": "Activity", "name": "Activity",
             "removed": False, "labels": ["run"]},
        ],
    }

    def test_csv_upload(self, ctrl, seeder):
        pid = seeder.project()
        uid = str(ObjectId())
        meta = ctrl.CSVUpload(self.upload_file(self.CSV_CONTENT), dict(self.CONFIG), pid, uid)
        assert meta["name"] == "upload"  # derived from file name (.csv stripped)
        doc = seeder.datasets.find_one({})
        assert doc["timeSeries"][0]["name"] == "temp_new"
        assert doc["labelings"][0]["labels"][0]["start"] == 100

    def test_csv_upload_named_and_latin1_fallback(self, ctrl, seeder):
        pid = seeder.project()
        config = dict(self.CONFIG)
        config["name"] = "mydata"
        # invalid utf-8 byte lives in an ignorable extra column -> latin-1 fallback
        bad_utf8 = (b"time,sensor_temp[m/s],junk\xfc\n"
                    b"100.500,1.0,x\n"
                    b"101.600,2.0,y\n"
                    b"102.700,3.0,z\n")
        meta = ctrl.CSVUpload(self.upload_file(bad_utf8), config, pid, str(ObjectId()))
        assert meta["name"] == "mydata"

    def test_csv_upload_empty_file_rejected(self, ctrl, seeder):
        pid = seeder.project()
        config = dict(self.CONFIG)
        config["timeSeries"][0]["removed"] = True
        with pytest.raises(HTTPException):
            ctrl.CSVUpload(self.upload_file(b"time\n1690000100.5\n"), config, pid, str(ObjectId()))


class TestUploadDatasetDevice:
    class FakeUploadFile:
        def __init__(self, content: bytes):
            self.content = content

        async def read(self):
            return self.content

    INFO = {
        "name": "device-ds",
        "files": [{"name": "f1.csv", "size": 10, "drop": [], "time": ["time"]}],
        "labeling": {"name": "L", "labels": [{"start": "1", "end": "2", "name": "on"}]},
        "metaData": {"k": "v"},
        "saveRaw": False,
    }

    DEVICE_CSV = b"time,temp\n1690000100.500,1.0\n1690000200.750,2.0\n"

    def test_success_with_labeling(self, ctrl, seeder):
        pid = str(seeder.project())
        files = [self.FakeUploadFile(self.DEVICE_CSV)]
        res = run_async(ctrl.uploadDatasetDevice(dict(self.INFO), files, pid, str(ObjectId())))
        assert res is True
        assert seeder.datasets.count_documents({}) == 1
        doc = seeder.datasets.find_one({})
        assert doc["name"] == "device-ds"
        assert doc["timeSeries"][0]["name"].endswith("_temp")
        assert doc["labelings"][0]["labels"][0]["type"]

    def test_success_without_labeling(self, ctrl, seeder):
        info = dict(self.INFO)
        info.pop("labeling")
        pid = str(seeder.project())
        res = run_async(ctrl.uploadDatasetDevice(info, [self.FakeUploadFile(self.DEVICE_CSV)],
                                                 pid, str(ObjectId())))
        assert res is True
        assert seeder.datasets.find_one({})["labelings"] == []

    def test_bad_csv_cleans_up_binaries(self, ctrl, seeder):
        from internal.config import TSDATA
        import os
        info = dict(self.INFO)
        info["labeling"] = None
        pid = str(seeder.project())
        before = set(os.listdir(TSDATA))
        with pytest.raises(HTTPException) as e:
            run_async(ctrl.uploadDatasetDevice(info, [self.FakeUploadFile(b"time,temp\nxx,yy\n")],
                                               pid, str(ObjectId())))
        assert e.value.status_code == 500
        after = set(os.listdir(TSDATA))
        assert before == after  # no orphan binaries left behind


class TestGenerateLabeling:
    def test_generate(self, ctrl, seeder):
        from controller.dataset_controller import CsvLabeling, CSVLabel
        labeling = CsvLabeling(name="L", labels=[CSVLabel(start="1", end="2", name="on")])
        res = ctrl.generateLabeling(str(seeder.project()), labeling)
        assert any(x["name"] == "on" for x in res["labels"])


class TestReceiveFileInfoAndCSV:
    def test_receives_until_total_size(self, ctrl):
        class FakeWS:
            def __init__(self):
                info = {"name": "wsds",
                        "files": [{"name": "a.csv", "size": 6, "drop": [], "time": ["time"]}],
                        "labeling": None, "metaData": {}, "saveRaw": False}
                import json as _json
                self.texts = [_json.dumps(info)]
                self.chunks = [b"hello!"]

            async def receive_text(self):
                return self.texts.pop(0)

            async def receive_bytes(self):
                return self.chunks.pop(0)

        from controller.dataset_controller import CSVDatasetInfo, FileDescriptor
        ws = FakeWS()
        info, data = run_async(ctrl.receiveFileInfoAndCSV(
            ws, "p", "u", dataModel=CSVDatasetInfo))
        assert isinstance(info, CSVDatasetInfo)
        assert data == b"hello!"

    def test_split_meta_data(self, ctrl):
        ts = {"name": "a", "data": [1, 2]}
        meta, values = ctrl._splitMeta_Data(ts)
        assert meta == {"name": "a"}
        assert values == [1, 2]


def run_async(coro):
    import asyncio
    return asyncio.run(coro)
