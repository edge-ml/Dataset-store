import io

import h5py
import numpy as np
import pytest
from unittest.mock import MagicMock, patch

import botocore.exceptions
from fastapi import HTTPException

from dataLoader.FileSystemDataLoader import FileSystemDataLoader
from dataLoader.S3DataLoader import S3DataLoader


class TestFileSystemDataLoader:
    def test_roundtrip_and_delete(self):
        loader = FileSystemDataLoader()
        time_arr = np.array([1, 2, 3], dtype=np.uint64)
        data_arr = np.array([0.5, 0.7, 0.9], dtype=np.float32)
        loader.save_series("abc", time_arr, data_arr)

        t, d = loader.load_series("abc")
        assert t.dtype == np.uint64 and d.dtype == np.float32
        assert t.tolist() == [1, 2, 3]
        assert np.allclose(d, data_arr)

        loader.delete("abc")
        with pytest.raises(FileNotFoundError):
            loader.load_series("abc")


def client_error(code):
    return botocore.exceptions.ClientError({"Error": {"Code": code, "Message": "m"}},
                                           "HeadBucket")


def make_loader(head_side_effect=None, create_side_effect=None):
    with patch("dataLoader.S3DataLoader.boto3.session") as m_session:
        client = MagicMock()
        m_session.Session.return_value.client.return_value = client
        if head_side_effect is not None:
            client.head_bucket.side_effect = head_side_effect
        if create_side_effect is not None:
            client.create_bucket.side_effect = create_side_effect
        loader = S3DataLoader()
    return loader, client


def h5_bytes(time_arr, data_arr):
    buf = io.BytesIO()
    with h5py.File(buf, "w") as f:
        f.create_dataset("time", data=time_arr)
        f.create_dataset("data", data=data_arr)
    return buf.getvalue()


class TestS3DataLoaderInit:
    def test_ok(self):
        loader, client = make_loader()
        client.head_bucket.assert_called_once()

    def test_404_creates_bucket(self):
        loader, client = make_loader(head_side_effect=client_error("404"))
        client.create_bucket.assert_called_once()

    def test_404_create_fails_logged_not_raised(self):
        loader, client = make_loader(
            head_side_effect=client_error("404"),
            create_side_effect=botocore.exceptions.BotoCoreError())
        # creation failure is only logged
        assert loader.s3 is not None

    def test_other_client_error_raises(self):
        with pytest.raises(botocore.exceptions.ClientError):
            make_loader(head_side_effect=client_error("403"))


@pytest.fixture
def s3():
    loader, client = make_loader()
    yield loader, client


class TestS3LoadSeries:
    def test_success(self, s3):
        loader, client = s3
        body = MagicMock(read=MagicMock(return_value=h5_bytes(np.array([1, 2]), np.array([1., 2.]))))
        client.get_object.return_value = {"Body": body}
        t, d = loader.load_series("id1")
        assert t.tolist() == [1, 2]
        assert np.allclose(d, [1., 2.])
        client.get_object.assert_called_once_with(Bucket=None, Key="id1")

    @pytest.mark.parametrize("exc", [
        botocore.exceptions.EndpointConnectionError(endpoint_url="http://x"),
        botocore.exceptions.ConnectTimeoutError(endpoint_url="http://x"),
    ])
    def test_connection_errors_map_to_504(self, s3, exc):
        loader, client = s3
        client.get_object.side_effect = exc
        with pytest.raises(HTTPException) as e:
            loader.load_series("id1")
        assert e.value.status_code == 504

    def test_client_error_returns_none(self, s3):
        loader, client = s3
        client.get_object.side_effect = botocore.exceptions.ClientError({"Error": {}}, "GetObject")
        assert loader.load_series("id1") == (None, None)

    def test_corrupt_hdf5_returns_none(self, s3):
        loader, client = s3
        body = MagicMock(read=MagicMock(return_value=b"this-is-not-hdf5"))
        client.get_object.return_value = {"Body": body}
        assert loader.load_series("id1") == (None, None)

    def test_unexpected_error_reraised(self, s3):
        loader, client = s3
        client.get_object.side_effect = RuntimeError("boom")
        with pytest.raises(RuntimeError):
            loader.load_series("id1")


class TestS3SaveSeries:
    def test_success(self, s3):
        loader, client = s3
        loader.save_series("id2", np.array([5, 6]), np.array([5., 6.]))
        kwargs = client.put_object.call_args.kwargs
        buf = io.BytesIO(kwargs["Body"])
        with h5py.File(buf, "r") as f:
            assert f["time"][:].tolist() == [5, 6]
            assert f["data"][:].tolist() == [5., 6.]

    @pytest.mark.parametrize("exc", [
        botocore.exceptions.EndpointConnectionError(endpoint_url="http://x"),
        botocore.exceptions.ConnectTimeoutError(endpoint_url="http://x"),
    ])
    def test_connection_errors_map_to_504(self, s3, exc):
        loader, client = s3
        client.put_object.side_effect = exc
        with pytest.raises(HTTPException) as e:
            loader.save_series("id2", np.array([]), np.array([]))
        assert e.value.status_code == 504

    def test_client_error_swallowed(self, s3):
        loader, client = s3
        client.put_object.side_effect = botocore.exceptions.ClientError({"Error": {}}, "PutObject")
        loader.save_series("id2", np.array([]), np.array([]))  # must not raise

    def test_hdf5_error_swallowed(self, s3):
        loader, client = s3
        with patch("dataLoader.S3DataLoader.h5py.File", side_effect=OSError("disk full")):
            loader.save_series("id2", np.array([]), np.array([]))

    def test_unexpected_error_reraised(self, s3):
        loader, client = s3
        client.put_object.side_effect = RuntimeError("boom")
        with pytest.raises(RuntimeError):
            loader.save_series("id2", np.array([]), np.array([]))


class TestS3Delete:
    def test_success(self, s3):
        loader, client = s3
        loader.delete("id3")
        client.delete_object.assert_called_once_with(Bucket=None, Key="id3")

    def test_client_error_swallowed(self, s3):
        loader, client = s3
        client.delete_object.side_effect = botocore.exceptions.ClientError({"Error": {}}, "DeleteObject")
        loader.delete("id3")

    def test_unexpected_error_reraised(self, s3):
        loader, client = s3
        client.delete_object.side_effect = RuntimeError("boom")
        with pytest.raises(RuntimeError):
            loader.delete("id3")
