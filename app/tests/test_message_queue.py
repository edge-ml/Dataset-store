import json
from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest
from bson.objectid import ObjectId

import MessageQueue as mq


class FakeMessage:
    def __init__(self, body: dict):
        self.body = json.dumps(body).encode()

    @asynccontextmanager
    async def process(self):
        yield


class FakeQueueIterator:
    def __init__(self, messages):
        self._messages = list(messages)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def __aiter__(self):
        self._it = iter(self._messages)
        return self

    async def __anext__(self):
        try:
            return next(self._it)
        except StopIteration:
            raise StopAsyncIteration


class FakeQueue:
    def __init__(self, messages):
        self._messages = messages

    def iterator(self):
        return FakeQueueIterator(self._messages)


class FakeChannel:
    async def declare_queue(self, name, **kwargs):
        return FakeQueue(MESSAGES.pop(0))


class FakeConnection:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def channel(self):
        return FakeChannel()


MESSAGES = []


def test_message_queue_processes_project_delete(seeder, tmp_path):
    from controller.dataset_controller import DatasetController
    from dataLoader.FileSystemDataLoader import FileSystemDataLoader

    seeder.project()
    # seed a dataset whose binary series exists on disk so deletion is exercised
    doc = seeder.dataset(time_series=[{"name": "a", "start": 0, "end": 1}])
    loader = FileSystemDataLoader()
    loader.save_series(str(doc["timeSeries"][0]["_id"]), [1], [1.0])
    path = f"{str(doc['timeSeries'][0]['_id'])}.bin"
    import os
    assert os.path.exists(os.path.join(os.environ["TSDATA"], path))

    MESSAGES.append([
        FakeMessage({"command": "projectDelete", "payload": str(seeder.project_id)}),
        FakeMessage({"command": "unknownCommand", "payload": "x"}),  # ignored branch
    ])

    async def fake_connect_robust(uri, loop=None):
        return FakeConnection()

    with patch.object(mq.aio_pika, "connect_robust", fake_connect_robust):
        import asyncio
        asyncio.run(mq.main(asyncio.new_event_loop()))

    # datasets and labelings for the project are gone, binaries deleted
    assert seeder.datasets.count_documents({}) == 0
    assert not os.path.exists(os.path.join(os.environ["TSDATA"], path))
