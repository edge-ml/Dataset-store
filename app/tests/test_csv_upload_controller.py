import asyncio
import json
import os

import pytest
from bson.objectid import ObjectId
from fastapi import BackgroundTasks, HTTPException

from controller.binary_store import BinaryStore
from controller.csv_uploadController import (
    _processData, generateLabeling, get_status, registerDownload)
from db.async_device_upload import AsyncUploadDB, UploadRequest


GOOD_CSV = b"time,temp\n1690000100.5,1.0\n1690000200.75,2.0\n"
BAD_CSV = b"time,temp\nnot-a-time,1.0\n"


class FakeUploadFile:
    def __init__(self, content: bytes, filename="f1.csv"):
        self.content = content
        self.filename = filename
        self._read_done = False

    async def read(self, size=-1):
        if self._read_done:
            return b""
        self._read_done = True
        return self.content

    async def seek(self, pos):
        if pos == 0:
            self._read_done = False
        return None


def make_info(**overrides):
    info = {
        "name": "async-ds",
        "files": [{"name": "f1.csv", "size": 10, "drop": [], "time": ["time"]}],
        "labeling": {"name": "L", "labels": [{"start": "1", "end": "2", "name": "on"}]},
        "metaData": {"k": "v"},
        "saveRaw": False,
    }
    info.update(overrides)
    return info


def run(coro):
    return asyncio.run(coro)


class TestProcessData:
    def test_success(self, seeder):
        seeder.project()
        pid = str(seeder.project_id)
        uid = str(ObjectId())
        upload_id = registerDownload(make_info(), [FakeUploadFile(GOOD_CSV)],
                                     pid, uid, BackgroundTasks())
        res = run(_processData(make_info(), [FakeUploadFile(GOOD_CSV)], pid, uid, upload_id))
        assert res is True
        assert AsyncUploadDB().getStatus(upload_id, ObjectId(uid)).status == 100

    def test_save_raw_writes_files(self, seeder, tmp_path):
        import internal.config as config
        pid = str(seeder.project_id)
        uid = str(ObjectId())
        info = make_info(saveRaw=True)
        upload_id = "raw-1"
        AsyncUploadDB().add_upload_request(UploadRequest(_id=upload_id, user_id=ObjectId(uid)))
        res = run(_processData(info, [FakeUploadFile(GOOD_CSV)], pid, uid, upload_id))
        assert res is True
        folder = os.path.join(config.RAW_UPLOAD_DATA, info["name"])
        assert os.path.exists(os.path.join(folder, "metadata.json"))
        assert os.path.exists(os.path.join(folder, "f1.csv"))

    def test_folder_already_exists(self, seeder):
        import internal.config as config
        pid = str(seeder.project_id)
        uid = str(ObjectId())
        info = make_info(saveRaw=True)
        folder = os.path.join(config.RAW_UPLOAD_DATA, info["name"])
        os.makedirs(folder)  # pre-existing -> rejected with 409 via status
        upload_id = "raw-2"
        AsyncUploadDB().add_upload_request(UploadRequest(_id=upload_id, user_id=ObjectId(uid)))
        with pytest.raises(Exception):
            run(_processData(info, [FakeUploadFile(GOOD_CSV)], pid, uid, upload_id))
        assert AsyncUploadDB().getStatus(upload_id, ObjectId(uid)).error == "Folder already exists"

    def test_bad_csv_cleans_binaries_and_sets_error(self, seeder):
        from internal.config import TSDATA
        pid = str(seeder.project_id)
        uid = str(ObjectId())
        before = set(os.listdir(TSDATA))
        upload_id = "bad-1"
        AsyncUploadDB().add_upload_request(UploadRequest(_id=upload_id, user_id=ObjectId(uid)))
        with pytest.raises(Exception):
            run(_processData(make_info(labeling=None), [FakeUploadFile(BAD_CSV)], pid, uid, upload_id))
        assert set(os.listdir(TSDATA)) == before
        assert AsyncUploadDB().getStatus(upload_id, ObjectId(uid)).error != ""

    def test_add_dataset_failure_cleans_binaries(self, seeder, monkeypatch):
        from internal.config import TSDATA
        from controller.csv_uploadController import dbm as upload_dbm
        pid = str(seeder.project_id)
        uid = str(ObjectId())
        before = set(os.listdir(TSDATA))

        def boom(*a, **k):
            raise RuntimeError("db down")

        monkeypatch.setattr(upload_dbm, "addDataset", boom)
        upload_id = "bad-2"
        AsyncUploadDB().add_upload_request(UploadRequest(_id=upload_id, user_id=ObjectId(uid)))
        with pytest.raises(RuntimeError):
            run(_processData(make_info(), [FakeUploadFile(GOOD_CSV)], pid, uid, upload_id))
        assert set(os.listdir(TSDATA)) == before


class TestGetStatus:
    def test_ok_and_error_branches(self, seeder):
        pid = str(seeder.project_id)
        uid = str(ObjectId())
        upload_id = registerDownload(make_info(), [FakeUploadFile(GOOD_CSV)],
                                     pid, uid, BackgroundTasks())
        assert get_status(upload_id, ObjectId(uid))["status"] == 0

    def test_error_500(self, seeder):
        uid = str(ObjectId())
        AsyncUploadDB().add_upload_request(UploadRequest(_id="err", user_id=ObjectId(uid)))
        AsyncUploadDB().setError("err", "boom")
        with pytest.raises(HTTPException) as e:
            get_status("err", ObjectId(uid))
        assert e.value.status_code == 500

    def test_folder_exists_409(self, seeder):
        uid = str(ObjectId())
        AsyncUploadDB().add_upload_request(UploadRequest(_id="dup", user_id=ObjectId(uid)))
        AsyncUploadDB().setError("dup", "Folder already exists")
        with pytest.raises(HTTPException) as e:
            get_status("dup", ObjectId(uid))
        assert e.value.status_code == 409


class TestGenerateLabeling:
    def test_generate(self, seeder):
        from controller.csv_uploadController import CSVLabel, CsvLabeling
        labeling = CsvLabeling(name="L", labels=[CSVLabel(start="1", end="2", name="on")])
        res = generateLabeling(str(seeder.project()), labeling)
        assert any(l["name"] == "on" for l in res["labels"])
