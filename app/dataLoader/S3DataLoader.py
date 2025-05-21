from dataLoader.BaseDataLoader import BaseDataLoader
from internal.config import S3_URL, S3_BUCKET_NAME, S3_ACCESS_KEY, S3_SECRET_KEY
import boto3
import h5py
import numpy as np
from io import BytesIO
import botocore.exceptions
from fastapi import HTTPException


class S3DataLoader(BaseDataLoader):
    def __init__(self):
        self.s3 = boto3.client(
            service_name="s3",
            endpoint_url=S3_URL,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
        )
        try:
            self.s3.head_bucket(Bucket=S3_BUCKET_NAME)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                print(f"Bucket {S3_BUCKET_NAME} does not exist. Creating!")
                try:
                    self.s3.create_bucket(Bucket=S3_BUCKET_NAME)
                except botocore.exceptions.BotoCoreError as err:
                    print(f"Failed to create bucket: {err}")
            else:
                print(f"Error accessing bucket: {e}")
                raise

    def load_series(self, id):
        try:
            print(f"Loading object {id} from S3 bucket {S3_BUCKET_NAME}")
            obj = self.s3.get_object(Bucket=S3_BUCKET_NAME, Key=id)
            buffer = BytesIO(obj["Body"].read())
            print(f"Object {id} loaded successfully")
            with h5py.File(buffer, "r") as f:
                time_arr = np.array(f["time"])
                data_arr = np.array(f["data"])
            return time_arr, data_arr
        except botocore.exceptions.EndpointConnectionError as e:
            raise HTTPException(status_code=504, detail=f"Timeout while retrieving object {id}: {e}")
        except botocore.exceptions.ConnectTimeoutError as e:
            raise HTTPException(status_code=504, detail=f"Timeout while retrieving object {id}: {e}")
        except botocore.exceptions.ClientError as e:
            print(f"Failed to retrieve object {id}: {e}")
            return None, None
        except OSError as e:
            print(f"Error reading HDF5 file for {id}: {e}")
            return None, None

    def save_series(self, id, time_arr, data_arr):
        try:
            buffer = BytesIO()
            with h5py.File(buffer, "w") as f:
                f.create_dataset("time", data=time_arr)
                f.create_dataset("data", data=data_arr)
            buffer.seek(0)
            self.s3.put_object(Bucket=S3_BUCKET_NAME, Key=id, Body=buffer.getvalue())
        except botocore.exceptions.EndpointConnectionError as e:
            raise HTTPException(status_code=504, detail=f"Timeout while saving object {id}: {e}")
        except botocore.exceptions.ConnectTimeoutError as e:
            raise HTTPException(status_code=504, detail=f"Timeout while saving object {id}: {e}")
        except botocore.exceptions.ClientError as e:
            print(f"Failed to save object {id}: {e}")
        except OSError as e:
            print(f"Error writing HDF5 file for {id}: {e}")

    def delete(self, id):
        try:
            self.s3.delete_object(Bucket=S3_BUCKET_NAME, Key=id)
        except botocore.exceptions.ClientError as e:
            print(f"Failed to delete object {id}: {e}")
