import asyncio
import os
import threading

import pytest
from bson.objectid import ObjectId

from controller.binary_store import BinaryStore
from controller.dataset_controller import DatasetController
from controller.downloadController import (
    delete_old_items, get_status, schedule_delete_task)
from dataLoader.BaseDataLoader import BaseDataLoader


class TestDatasetControllerInit:
    def test_creates_data_dir(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        assert not (tmp_path / "DATA").exists()
        DatasetController()
        assert (tmp_path / "DATA").exists()


class TestBaseDataLoader:
    def test_methods_not_implemented(self):
        base = BaseDataLoader()
        for call in (lambda: base.load_series("x"),
                     lambda: base.save_series("x", [], []),
                     lambda: base.delete("x")):
            with pytest.raises(NotImplementedError):
                call()


class TestLockManager:
    def test_sequential_and_nested_ids(self):
        from utils.InMemoryLockManager import thread_safe
        with thread_safe("id-1"):
            # same id re-acquired sequentially is fine
            pass
        # different ids can be held at the same time
        with thread_safe("a"), thread_safe("b"):
            pass

    def test_mutual_exclusion_same_id(self):
        from utils.InMemoryLockManager import thread_safe, locks
        order = []
        release = threading.Event()

        def blocker():
            with thread_safe("shared"):
                order.append("blocker-in")
                release.wait(timeout=5)
                order.append("blocker-out")

        t = threading.Thread(target=blocker)
        t.start()
        while "blocker-in" not in order:
            pass
        acquired = locks["shared"].acquire(timeout=0.2)
        if acquired:
            locks["shared"].release()
        assert not acquired  # second acquire of the same id blocks
        release.set()
        t.join(timeout=5)


class TestDownloadControllerExtras:
    def test_get_status_key_error_maps_to_404(self, monkeypatch):
        from controller import downloadController as dc

        def boom(user_id):
            raise KeyError()

        monkeypatch.setattr(dc.db, "get_by_user", boom)
        import fastapi
        with pytest.raises(fastapi.HTTPException) as e:
            asyncio.run(get_status(ObjectId()))
        assert e.value.status_code == 404

    def test_schedule_delete_task_runs(self, seeder):
        import datetime
        from db.csv import csvDB, DBEntryDataset
        seeder.project()
        f = seeder.downloads and None
        import tempfile
        fh = tempfile.NamedTemporaryFile(delete=False)
        fh.close()
        db = csvDB()
        db.add(DBEntryDataset(downloadId="old-one", projectId=ObjectId(),
                              userId=ObjectId(), projectName="p", datasetName="d",
                              filePath=fh.name))
        db.col.update_one({"downloadId": "old-one"},
                          {"$set": {"created_at": datetime.datetime.utcnow() - datetime.timedelta(hours=9)}})

        async def run_once():
            task = asyncio.create_task(schedule_delete_task(interval=0.01))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(run_once())
        os.remove(fh.name) if os.path.exists(fh.name) else None
