from starlette.config import Config


config = Config(".env")

default_values = {
    "S3_URL": None,
    "S3_BUCKET_NAME": None,
    "S3_ACCESS_KEY": None,
    "S3_SECRET_KEY": None,
    "S3_TIMEOUT_SECONDS": 2  # Default to 5 seconds, as a compromise
}

required_values = [
    "MONGO_URI",
    "PROJECT_DBNAME",
    "PROJECT_COLLNAME",
    "DATASTORE_DBNAME",
    "DATASTORE_COLLNAME",
    "TIMESERIES_DBNAME",
    "TIMESERIES_COLLNAME",
    "LABELING_COLLNAME",
    "DEVICE_API_COLLNAME",
    "CSV_COLLNAME",
    "ASYNC_UPLOAD_COLNAME",
    "SECRET_KEY",
    "TS_STORE_MECHANISM",
    "TSDATA",
    "RAW_UPLOAD_DATA",
]

for key in required_values:
    globals()[key] = config(key)

for variable, default_value in default_values.items():
    if variable not in globals():
        globals()[variable] = config(variable, default=default_value)
