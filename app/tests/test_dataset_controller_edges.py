"""Branch-completion tests for DatasetController edge paths."""
import asyncio
import io
import os

import numpy as np
import pytest
from bson.objectid import ObjectId

from controller.binary_store import BinaryStore
from controller.dataset_controller import CSVDatasetInfo, DatasetController


@pytest.fixture
def ctrl(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # DATA dir created inside tmp cwd
    return DatasetController()


def run(coro):
    return asyncio.run(coro)


class TestAddDatasetZipBranch:
    def test_accepts_zip_object_as_data(self, ctrl, seeder):
        pid = seeder.project()
        body = {"name": "zipped", "userId": str(ObjectId()),
                "timeSeries": [{"name": "a", "data": zip([1000000000, 2000000000], [1.0, 2.0])}]}
        res = ctrl.addDataset(body, pid)
        stored = seeder.datasets.find_one({})
        assert stored["timeSeries"][0]["length"] == 2


class TestUploadDatasetDeviceEdgeCases:
    class FakeFile:
        def __init__(self, content):
            self.content = content
            self._done = False

        async def read(self, size=-1):
            if self._done:
                return b""
            self._done = True
            return self.content

    def _info(self, **kw):
        info = {"name": "d", "files": [{"name": "f.csv", "size": 10, "drop": [], "time": ["time"]}],
                "labeling": None, "metaData": None, "saveRaw": False}
        info.update(kw)
        return info

    GOOD = b"time,temp\n1690000100.5,1\n1690000200.5,2\n"
    BAD = b"no-time-column-here\nx,y\n"

    def test_all_empty_files_rejected(self, ctrl, seeder):
        from fastapi import HTTPException
        pid = str(seeder.project())
        # a csv whose only column is the time column -> no sensor data at all
        empty = self.FakeFile(b"time\n1690000100.5\n")
        # regression: crashed with ValueError (min() of empty list) -> opaque 500
        with pytest.raises(HTTPException) as e:
            run(ctrl.uploadDatasetDevice(self._info(), [empty], pid, "u"))
        assert e.value.status_code == 400

    def test_second_file_failure_cleans_first_binaries(self, ctrl, seeder):
        from internal.config import TSDATA
        pid = str(seeder.project())
        before = set(os.listdir(TSDATA))
        files = [self.FakeFile(self.GOOD), self.FakeFile(self.BAD)]
        with pytest.raises(Exception):
            run(ctrl.uploadDatasetDevice(self._info(), files, pid, "u"))
        assert set(os.listdir(TSDATA)) == before

    def test_add_dataset_failure_cleans_binaries(self, ctrl, seeder, monkeypatch):
        from internal.config import TSDATA
        pid = str(seeder.project())
        before = set(os.listdir(TSDATA))

        def boom(*a, **k):
            raise RuntimeError("db down")

        monkeypatch.setattr(ctrl.dbm, "addDataset", boom)
        with pytest.raises(Exception):
            run(ctrl.uploadDatasetDevice(self._info(), [self.FakeFile(self.GOOD)], pid, "u"))
        assert set(os.listdir(TSDATA)) == before


class TestReceiveFileInfoMultiChunk:
    def test_multiple_chunks_until_total_size(self, ctrl):
        import json as _json
        info = {"name": "ws", "files": [{"name": "a.csv", "size": 11,
                                         "drop": [], "time": ["time"]}],
                "labeling": None, "metaData": {}, "saveRaw": False}

        class FakeWS:
            def __init__(self):
                self.texts = [_json.dumps(info)]
                self.chunks = [b"hello", b"world!", b"NEVER"]

            async def receive_text(self):
                return self.texts.pop(0)

            async def receive_bytes(self):
                if len(self.chunks) > 1:
                    return self.chunks.pop(0)
                raise AssertionError("should have stopped after total size")

        got_info, data = run(ctrl.receiveFileInfoAndCSV(FakeWS(), "p", "u",
                                                        dataModel=CSVDatasetInfo))
        assert data == b"helloworld!"  # exactly total_size bytes consumed


def make_csv_dataset(ctrl, seeder, two_sensors=True):
    """Seed dataset + labelings + binaries for getCSV tests."""
    pid = str(seeder.project_id)
    ts_defs = [("a", [[1000000000, 1.0], [2000000000, 2.0], [3000000000, 3.0]])]
    if two_sensors:
        ts_defs.append(("b", [[1000000000, 10.0], [2000000000, 20.0], [3000000000, 30.0]]))
    time_series = []
    for name, data in ts_defs:
        ts_id = ObjectId()
        BinaryStore(str(ts_id))._appendValues([row[0] for row in data],
                                              [row[1] for row in data])
        time_series.append({"_id": ts_id, "name": name, "start": data[0][0], "end": data[-1][0]})
    labeling_id = ObjectId()
    label_type = ObjectId()
    seeder.labelings.insert_one({
        "_id": labeling_id, "projectId": seeder.project_id, "name": "Activity",
        "labels": [{"_id": label_type, "name": "run", "color": "#000000"}]})
    doc = {
        "_id": ObjectId(), "name": "csv-ds", "projectId": seeder.project_id,
        "userId": ObjectId(), "metaData": {}, "start": 1000000000, "end": 3000000000,
        "timeSeries": time_series,
        "labelings": [{"labelingId": labeling_id,
                       "labels": [{"type": label_type, "start": 1000000000, "end": 2000000000}]}],
    }
    seeder.datasets.insert_one(doc)
    return doc, labeling_id, label_type


class TestGetCSVFull:
    def test_merge_and_labels(self, ctrl, seeder):
        doc, labeling_id, label_type = make_csv_dataset(ctrl, seeder)
        stream, fileName = ctrl.getCSV(str(seeder.project_id), doc["_id"])
        text = stream.getvalue()
        lines = text.strip().splitlines()
        assert fileName == "csv-ds"
        assert lines[0].startswith("time")
        # both sensors merged into one frame
        assert any("sensor_a" in col for col in lines[0].split(","))
        assert any("sensor_b" in col for col in lines[0].split(","))
        # label column marks rows within [1e9, 2e9] as 'x'
        label_col_idx = [i for i, c in enumerate(lines[0].split(",")) if c.startswith("label_Activity")]
        assert label_col_idx
        marked = [line.split(",")[label_col_idx[0]] for line in lines[1:]]
        assert "x" in marked and "" in marked

    def test_missing_labeling_definition_skipped(self, ctrl, seeder):
        # regression: a dataset referencing a deleted labeling crashed getCSV
        doc, _, _ = make_csv_dataset(ctrl, seeder)
        seeder.labelings.delete_many({})
        stream, fileName = ctrl.getCSV(str(seeder.project_id), doc["_id"])
        assert fileName == "csv-ds"
