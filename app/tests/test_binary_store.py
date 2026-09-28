import os

import h5py
import numpy as np
import pytest

from controller.binary_store import BinaryStore
from dataLoader.FileSystemDataLoader import FileSystemDataLoader


def make_store(n=5):
    store = BinaryStore("test-series")
    values = [[i * 10, float(i)] for i in range(n)]
    return store, values


class TestAppend:
    def test_append_basic(self):
        store, values = make_store()
        start, end, sampling_rate, length = store.append(values)
        assert start == 0 and end == 40
        assert length == 5
        assert sampling_rate["mean"] == pytest.approx(10)
        assert sampling_rate["var"] == 0

    def test_append_empty(self):
        store = BinaryStore("empty")
        start, end, sr, length = store.append([])
        assert start is None and end is None and length == 0

    def test_append_sorts_by_time(self):
        store = BinaryStore("sortme")
        store.append([[30, 3.0], [10, 1.0], [20, 2.0]])
        assert store.time_arr.tolist() == [10, 20, 30]
        assert store.data_arr.tolist() == [1.0, 2.0, 3.0]

    def test_sampling_rate_variance(self):
        store = BinaryStore("var")
        store.append([[0, 0.], [10, 1.], [25, 2.]])
        assert np.allclose(store.getFull()["time"].tolist(), [0, 10, 25])


class TestPersistence:
    def test_save_and_load(self):
        store = BinaryStore("persist")
        store.append([[1, 1.], [2, 2.]])
        other = BinaryStore("persist")
        other.loadSeries()
        assert other.time_arr.tolist() == [1, 2]

    def test_delete(self):
        store = BinaryStore("gone")
        store.append([[1, 1.]])
        path = os.path.join(os.environ["TSDATA"], "gone.bin")
        assert os.path.exists(path)
        BinaryStore("gone").delete()
        assert not os.path.exists(path)


class TestGetPart:
    def test_small_series_returns_everything(self):
        store = BinaryStore("small")
        store.append([[i, float(i)] for i in range(10)])
        res = store.getPart("undefined", "undefined", 5)
        assert res.shape == (10, 2)

    def test_large_series_with_range(self):
        store = BinaryStore("large")
        store.append([[i, float(i)] for i in range(500)])
        res = store.getPart(100, 200, "100000")  # resolution high: no downsampling
        assert res.shape[0] == 100
        assert res[0][0] == 100

    def test_downsampling_applied(self):
        import lttbc
        store = BinaryStore("downsample")
        store.append([[i, float(i % 7)] for i in range(1000)])
        res = store.getPart("undefined", "undefined", 50)
        assert res.shape == (50, 2)

    def test_no_resolution_no_downsampling(self):
        store = BinaryStore("nores")
        store.append([[i, float(i)] for i in range(300)])
        res = store.getPart("undefined", "undefined", None)
        assert res.shape == (299, 2)  # end index exclusive


class TestGetHdf5Stream:
    def test_stream_file_written(self):
        store = BinaryStore("hdf5stream")
        store.append([[7, 7.5]])
        path = store.getHdf5Stream()
        try:
            with h5py.File(path, "r") as f:
                assert f["time"][:].tolist() == [7]
                assert f["data"][:].tolist() == [pytest.approx(7.5)]
        finally:
            os.remove(path)

    def test_get_full(self):
        store = BinaryStore("fullcheck")
        store.append([[3, 4.5]])
        full = store.getFull()
        assert isinstance(full, dict) and "time" in full and "data" in full
