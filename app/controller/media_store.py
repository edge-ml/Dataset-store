"""Storage for audio and video files.

Recordings are stored as they were uploaded, next to the time-series blobs:
under `media/` in the series bucket (S3) or the series folder (FS). Unlike a
series they are never loaded whole; they are written from a file on disk and
read back in byte ranges.
"""
import os
import shutil

from internal.config import (
    TS_STORE_MECHANISM, TSDATA,
    S3_URL, S3_BUCKET_NAME, S3_ACCESS_KEY, S3_SECRET_KEY, MEDIA_S3_TIMEOUT_SECONDS,
)

CHUNK_SIZE = 1024 * 1024
PREFIX = "media"


class FileSystemMediaStore:

    def __init__(self, root=None):
        self.root = os.path.join(root or TSDATA, PREFIX)

    def _path(self, key):
        return os.path.join(self.root, key)

    def save_file(self, key, path):
        os.makedirs(self.root, exist_ok=True)
        # copy next to the target first so a half-written file is never served
        tmp = self._path(key) + ".part"
        shutil.copyfile(path, tmp)
        os.replace(tmp, self._path(key))

    def save_bytes(self, key, data):
        os.makedirs(self.root, exist_ok=True)
        tmp = self._path(key) + ".part"
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, self._path(key))

    def load_bytes(self, key):
        try:
            with open(self._path(key), "rb") as f:
                return f.read()
        except FileNotFoundError:
            return None

    def size(self, key):
        try:
            return os.path.getsize(self._path(key))
        except FileNotFoundError:
            return None

    def read(self, key, start, end):
        """Yield the bytes start..end (both inclusive)."""
        with open(self._path(key), "rb") as f:
            f.seek(start)
            remaining = end - start + 1
            while remaining > 0:
                chunk = f.read(min(CHUNK_SIZE, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                yield chunk

    def delete(self, key):
        try:
            os.remove(self._path(key))
        except FileNotFoundError:
            pass


class S3MediaStore:

    def __init__(self):
        self._client = None

    @property
    def s3(self):
        # The series client gives up after a couple of seconds, which is far too
        # short for files of this size, so recordings get their own client.
        if self._client is None:
            import boto3.session
            self._client = boto3.session.Session().client(
                service_name="s3",
                endpoint_url=S3_URL,
                aws_access_key_id=S3_ACCESS_KEY,
                aws_secret_access_key=S3_SECRET_KEY,
                config=boto3.session.Config(
                    retries={"max_attempts": 5, "mode": "standard"},
                    connect_timeout=10,
                    read_timeout=MEDIA_S3_TIMEOUT_SECONDS,
                ),
            )
        return self._client

    def _key(self, key):
        return f"{PREFIX}/{key}"

    def save_file(self, key, path):
        # upload_file switches to a multipart upload for large files
        self.s3.upload_file(path, S3_BUCKET_NAME, self._key(key))

    def save_bytes(self, key, data):
        self.s3.put_object(Bucket=S3_BUCKET_NAME, Key=self._key(key), Body=data)

    def load_bytes(self, key):
        import botocore.exceptions
        try:
            return self.s3.get_object(Bucket=S3_BUCKET_NAME, Key=self._key(key))["Body"].read()
        except botocore.exceptions.ClientError:
            return None

    def size(self, key):
        import botocore.exceptions
        try:
            return self.s3.head_object(Bucket=S3_BUCKET_NAME, Key=self._key(key))["ContentLength"]
        except botocore.exceptions.ClientError:
            return None

    def read(self, key, start, end):
        obj = self.s3.get_object(Bucket=S3_BUCKET_NAME, Key=self._key(key), Range=f"bytes={start}-{end}")
        body = obj["Body"]
        try:
            for chunk in body.iter_chunks(CHUNK_SIZE):
                yield chunk
        finally:
            body.close()

    def delete(self, key):
        self.s3.delete_object(Bucket=S3_BUCKET_NAME, Key=self._key(key))


mediaStore = S3MediaStore() if TS_STORE_MECHANISM == "S3" else FileSystemMediaStore()
