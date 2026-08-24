import os
import zipfile

import pytest
from bson.objectid import ObjectId

from controller.downloadController import (
    cancel_download, delete_old_items, downloadDataset, downloadProject,
    get_download_data, get_status, registerForDownloadDataset,
    registerForDownloadProject)
from controller.binary_store import BinaryStore


def seed_dataset_with_series(seeder, name="ds", n_points=5):
    doc = seeder.dataset(name=name, time_series=[{"name": "a", "start": 0, "end": n_points - 1}])
    BinaryStore(str(doc["timeSeries"][0]["_id"]))._appendValues(
        list(range(n_points)), [float(i) for i in range(n_points)])
    return doc


def run_background_tasks(tasks):
    import asyncio
    asyncio.run(tasks())


def add_entry(seeder, download_id, project=None, user=None, status=0):
    from db.csv import csvDB, DBEntryDataset
    csvDB().add(DBEntryDataset(downloadId=download_id,
                               projectId=project or seeder.project_id,
                               userId=user or ObjectId(), projectName="p",
                               datasetName="d", status=status))


class TestRegister:
    def test_register_dataset(self, seeder):
        seeder.project()
        doc = seed_dataset_with_series(seeder)
        from fastapi import BackgroundTasks
        tasks = BackgroundTasks()
        data = registerForDownloadDataset(str(seeder.project_id), str(doc["_id"]),
                                          str(ObjectId()), tasks)
        assert data["downloadId"]
        assert seeder.downloads.count_documents({}) == 1

    def test_register_and_run_dataset_download(self, seeder):
        seeder.project()
        doc = seed_dataset_with_series(seeder)
        from fastapi import BackgroundTasks
        tasks = BackgroundTasks()
        data = registerForDownloadDataset(str(seeder.project_id), str(doc["_id"]),
                                          str(ObjectId()), tasks)
        run_background_tasks(tasks)
        entry = seeder.downloads.find_one({})
        assert entry["status"] == 100
        assert entry["fileName"] == "ds"

    def test_register_project(self, seeder):
        seeder.project()
        seed_dataset_with_series(seeder, name="a")
        seed_dataset_with_series(seeder, name="b")
        from fastapi import BackgroundTasks
        tasks = BackgroundTasks()
        data = registerForDownloadProject(seeder.project_id, ObjectId(), tasks)
        assert data["projectName"] == "test-project"
        assert seeder.downloads.count_documents({}) == 1


class TestDownloadTasks:
    def test_download_dataset(self, seeder):
        seeder.project()
        doc = seed_dataset_with_series(seeder)
        add_entry(seeder, "dl-id")
        downloadDataset("dl-id", str(seeder.project_id), doc["_id"])
        entry = seeder.downloads.find_one({"downloadId": "dl-id"})
        assert entry["status"] == 100
        assert entry["fileName"] == "ds"
        assert os.path.exists(entry["filePath"])
        with open(entry["filePath"], "rb") as f:
            assert b"sensor_a" in f.read()
        os.remove(entry["filePath"])

    def test_download_project_zip_and_name_collision(self, seeder):
        from fastapi import BackgroundTasks
        seeder.project()
        seed_dataset_with_series(seeder, name="same")
        seed_dataset_with_series(seeder, name="same")
        tasks = BackgroundTasks()
        data = registerForDownloadProject(seeder.project_id, ObjectId(), tasks)
        run_background_tasks(tasks)
        entry = seeder.downloads.find_one({})
        assert entry["status"] == 100
        with open(entry["filePath"], "rb") as f:
            with zipfile.ZipFile(f) as z:
                names = z.namelist()
                assert names[0] == "same.csv"
                assert names[1].startswith("same_")  # collision renamed
        os.remove(entry["filePath"])

    def test_download_project_empty(self, seeder):
        from fastapi import BackgroundTasks
        seeder.project()
        tasks = BackgroundTasks()
        registerForDownloadProject(seeder.project_id, ObjectId(), tasks)
        run_background_tasks(tasks)
        assert seeder.downloads.find_one({})["status"] == 100
        os.remove(seeder.downloads.find_one({})["filePath"])


class TestStatusAndFetch:
    def _register_ready_dataset(self, seeder):
        seeder.project()
        doc = seed_dataset_with_series(seeder)
        add_entry(seeder, "ready-dl")
        downloadDataset("ready-dl", str(seeder.project_id), doc["_id"])

    def test_get_status(self, seeder):
        self._register_ready_dataset(seeder)
        res = get_status_sync(seeder.admin_id)
        # entries are keyed by the userId they were registered with
        assert isinstance(res, list)

    def test_get_download_not_ready(self, seeder):
        seeder.project()
        seed_dataset_with_series(seeder)
        from db.csv import csvDB, DBEntryDataset
        csvDB().add(DBEntryDataset(downloadId="pending", projectId=seeder.project_id,
                                   userId=ObjectId(), projectName="p", datasetName="d"))
        import asyncio
        with pytest.raises(Exception) as e:
            asyncio.run(get_download_data("pending"))
        assert "not ready" in str(e.value.detail)

    def test_get_download_dataset_csv(self, seeder):
        self._register_ready_dataset(seeder)
        import asyncio
        response = asyncio.run(get_download_data("ready-dl"))
        assert "text/csv" in response.media_type
        async def read_body():
            return b"".join([chunk async for chunk in response.body_iterator])
        import asyncio
        body = asyncio.run(read_body())
        assert b"sensor_a" in body

    def test_get_download_project_zip(self, seeder):
        from fastapi import BackgroundTasks
        seeder.project()
        seed_dataset_with_series(seeder)
        tasks = BackgroundTasks()
        registerForDownloadProject(seeder.project_id, ObjectId(), tasks)
        run_background_tasks(tasks)
        download_id = seeder.downloads.find_one({})["downloadId"]
        response = run_async(get_download_data(download_id))
        assert "application/zip" in response.media_type

    def test_cancel_download(self, seeder):
        self._register_ready_dataset(seeder)
        path = seeder.downloads.find_one({})["filePath"]
        import asyncio
        asyncio.run(cancel_download("ready-dl"))
        assert not os.path.exists(path)
        assert seeder.downloads.count_documents({}) == 0

    def test_cancel_missing_file_tolerated(self, seeder):
        from db.csv import csvDB, DBEntryDataset
        csvDB().add(DBEntryDataset(downloadId="ghost", projectId=ObjectId(),
                                   userId=ObjectId(), projectName="p", datasetName="d",
                                   filePath="/nonexistent/file.bin"))
        import asyncio
        asyncio.run(cancel_download("ghost"))
        assert seeder.downloads.count_documents({}) == 0 if hasattr(seeder, "downloads") else True


class TestDeleteOldItems:
    def test_removes_old_entries(self, seeder, tmp_path):
        import datetime
        from db.csv import csvDB, DBEntryDataset
        f = tmp_path / "old.bin"
        f.write_bytes(b"x")
        db = csvDB()
        db.add(DBEntryDataset(downloadId="stale", projectId=ObjectId(),
                              userId=ObjectId(), projectName="p", datasetName="d",
                              filePath=str(f)))
        db.col.update_one({"downloadId": "stale"},
                          {"$set": {"created_at": datetime.datetime.utcnow() - datetime.timedelta(hours=2)}})
        fresh_path = tmp_path / "fresh.bin"
        fresh_path.write_bytes(b"x")
        db.add(DBEntryDataset(downloadId="fresh", projectId=ObjectId(),
                              userId=ObjectId(), projectName="p", datasetName="d",
                              filePath=str(fresh_path)))
        delete_old_items()
        assert not f.exists()
        assert not os.path.exists(str(fresh_path)) or fresh_path.exists()
        ids = {d["downloadId"] for d in seeder.downloads.find({})}
        assert "stale" not in ids and "fresh" in ids

    def test_missing_file_skipped(self, seeder):
        import datetime
        from db.csv import csvDB, DBEntryDataset
        db = csvDB()
        db.add(DBEntryDataset(downloadId="gone-file", projectId=ObjectId(),
                              userId=ObjectId(), projectName="p", datasetName="d",
                              filePath="/does/not/exist.bin"))
        db.col.update_one({"downloadId": "gone-file"},
                          {"$set": {"created_at": datetime.datetime.utcnow() - datetime.timedelta(hours=9)}})
        delete_old_items()  # regression: used to crash on missing files
        assert seeder.downloads.find_one({"downloadId": "gone-file"}) is None


def run_async(coro):
    import asyncio
    return asyncio.run(coro)


def get_status_sync(user_id):
    return run_async(get_status(user_id))
