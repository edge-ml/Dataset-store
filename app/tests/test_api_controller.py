import os

import pytest
from bson.objectid import ObjectId

from controller.api_controller import appendDataset, initDataset
from controller.labelingController import getProjectLabelings
from routers.deviceApi import (
    IncrementUploadModal,
    TimeSeriesDataModel,
    InitDatasetModalLabeling,
)
from tests.conftest import fake_client, DATASTORE_DBNAME, DATASTORE_COLLNAME


@pytest.fixture
def setup(seeder):
    pid = str(seeder.project())
    user = str(ObjectId())
    ds_id = initDataset("apid", ["a", "b"], {"k": "v"}, user, pid)
    return seeder, pid, user, ds_id


class TestInitDataset:
    def test_creates_dataset(self, setup):
        seeder, pid, user, ds_id = setup
        doc = seeder.datasets.find_one({})
        assert str(doc["_id"]) == ds_id
        for t in doc["timeSeries"]:
            assert t["start"] == 2**63 - 1 and t["end"] == -1
        # an empty binary series is created for every timeseries
        for t in doc["timeSeries"]:
            assert os.path.exists(os.path.join(os.environ["TSDATA"], f"{t['_id']}.bin"))


class TestAppendDataset:
    def test_appends_data_and_updates_bounds(self, setup):
        seeder, pid, user, ds_id = setup
        body = IncrementUploadModal(
            data=[TimeSeriesDataModel(name="a", data=[[1000, 1.0], [2000, 2.0]])])
        appendDataset(body, user, pid, ds_id)

        doc = seeder.datasets.find_one({})
        ts_a = next(t for t in doc["timeSeries"] if t["name"] == "a")
        assert ts_a["length"] == 2
        assert ts_a["end"] == 2000
        # series-level end is the max of appended timestamps (regression: used min)
        assert max(t["end"] for t in doc["timeSeries"]) == 2000

    def test_with_labeling(self, setup):
        seeder, pid, user, ds_id = setup
        body = IncrementUploadModal(
            data=[TimeSeriesDataModel(name="b", data=[[5000, 3.0]])],
            labeling=InitDatasetModalLabeling(labelingName="L", labelName="run"),
        )
        appendDataset(body, user, pid, ds_id)  # must not raise
        labelings = getProjectLabelings(pid)
        assert any(l["name"] == "L" for l in labelings)

    def test_unknown_timeseries_swallowed(self, setup):
        seeder, pid, user, ds_id = setup
        bad = IncrementUploadModal(data=[TimeSeriesDataModel(name="zzz", data=[[1, 2.0]])])
        appendDataset(bad, user, pid, ds_id)  # exception swallowed by design
