from dataLoader.BaseDataLoader import BaseDataLoader
from internal.config import S3_URL, S3_BUCKET_NAME, S3_ACCESS_KEY, S3_SECRET_KEY, S3_TIMEOUT_SECONDS
import boto3
import h5py
import numpy as np
from io import BytesIO
import botocore.exceptions
from fastapi import HTTPException
import boto3.session
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3DataLoader(BaseDataLoader):
    def __init__(self):
        session = boto3.session.Session()
        self.s3 = session.client(
            service_name="s3",
            endpoint_url=S3_URL,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            config=boto3.session.Config(
                retries={
                    'max_attempts': 10,
                    'mode': 'standard'
                },
                connect_timeout=S3_TIMEOUT_SECONDS,
                read_timeout=S3_TIMEOUT_SECONDS
            )
        )
        try:
            self.s3.head_bucket(Bucket=S3_BUCKET_NAME)
            logger.info(f"Successfully connected to S3 bucket: {S3_BUCKET_NAME}")
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                logger.warning(f"Bucket {S3_BUCKET_NAME} does not exist. Attempting to create!")
                try:
                    self.s3.create_bucket(Bucket=S3_BUCKET_NAME)
                    logger.info(f"Successfully created S3 bucket: {S3_BUCKET_NAME}")
                except botocore.exceptions.BotoCoreError as err:
                    logger.error(f"Failed to create bucket {S3_BUCKET_NAME}: {err}")
            else:
                logger.error(f"Error accessing bucket {S3_BUCKET_NAME}: {e}")
                raise

    def load_series(self, id):
        start_time = time.time()
        try:
            logger.info(f"Attempting to load object {id} from S3 bucket {S3_BUCKET_NAME}")
            obj = self.s3.get_object(Bucket=S3_BUCKET_NAME, Key=id)
            buffer = BytesIO(obj["Body"].read())
            end_time = time.time()
            logger.info(f"Object {id} loaded successfully in {end_time - start_time:.2f} seconds.")
            with h5py.File(buffer, "r") as f:
                time_arr = np.array(f["time"])
                data_arr = np.array(f["data"])
            return time_arr, data_arr
        except botocore.exceptions.EndpointConnectionError as e:
            logger.error(f"EndpointConnectionError while retrieving object {id}: {e}")
            raise HTTPException(status_code=504, detail=f"Timeout while retrieving object {id}: {e}")
        except botocore.exceptions.ConnectTimeoutError as e:
            logger.error(f"ConnectTimeoutError while retrieving object {id}: {e}")
            raise HTTPException(status_code=504, detail=f"Timeout while retrieving object {id}: {e}")
        except botocore.exceptions.ClientError as e:
            logger.error(f"ClientError while retrieving object {id}: {e}")
            return None, None
        except OSError as e:
            logger.error(f"OSError (HDF5 file error) for {id}: {e}")
            return None, None
        except Exception as e:
            logger.error(f"An unexpected error occurred while loading object {id}: {e}")
            raise

    def save_series(self, id, time_arr, data_arr):
        start_time = time.time()
        try:
            logger.info(f"Attempting to save object {id} to S3 bucket {S3_BUCKET_NAME}")
            buffer = BytesIO()
            with h5py.File(buffer, "w") as f:
                f.create_dataset("time", data=time_arr)
                f.create_dataset("data", data=data_arr)
            buffer.seek(0)
            self.s3.put_object(Bucket=S3_BUCKET_NAME, Key=id, Body=buffer.getvalue())
            end_time = time.time()
            logger.info(f"Object {id} saved successfully in {end_time - start_time:.2f} seconds.")
        except botocore.exceptions.EndpointConnectionError as e:
            logger.error(f"EndpointConnectionError while saving object {id}: {e}")
            raise HTTPException(status_code=504, detail=f"Timeout while saving object {id}: {e}")
        except botocore.exceptions.ConnectTimeoutError as e:
            logger.error(f"ConnectTimeoutError while saving object {id}: {e}")
            raise HTTPException(status_code=504, detail=f"Timeout while saving object {id}: {e}")
        except botocore.exceptions.ClientError as e:
            logger.error(f"ClientError while saving object {id}: {e}")
        except OSError as e:
            logger.error(f"OSError (HDF5 file error) while saving object {id}: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while saving object {id}: {e}")
            raise

    def delete(self, id):
        try:
            logger.info(f"Attempting to delete object {id} from S3 bucket {S3_BUCKET_NAME}")
            self.s3.delete_object(Bucket=S3_BUCKET_NAME, Key=id)
            logger.info(f"Object {id} deleted successfully.")
        except botocore.exceptions.ClientError as e:
            logger.error(f"ClientError while deleting object {id}: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while deleting object {id}: {e}")
            raise
