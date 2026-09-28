from starlette.config import Config


config = Config(".env")

default_values = {
    "S3_URL": None,
    "S3_BUCKET_NAME": None,
    "S3_ACCESS_KEY": None,
    "S3_SECRET_KEY": None,
    "S3_TIMEOUT_SECONDS": 2,  # Default to 5 seconds, as a compromise
    # Auth (previously provided by the standalone authentication service)
    "DATABASE_COLLECTION_AUTH": "auth_dev",
    "SERVER_TTL": "1d",
    "SERVER_REFRESH_TTL": "1d",
    "SERVER_REFRESH_SECRET": None,  # falls back to SECRET_KEY
    "GITHUB_CLIENT_ID": None,
    "GITHUB_CLIENT_SECRET": None,
    "GITHUB_CALLBACK_URL": None,
    "HOST": "http://localhost",
    # Message broker (previously provided by the backend service)
    "RABBITMQ_URI": "amqp://guest:guest@localhost:5672",
    "RABBITMQ_QUEUE": "edgeml",
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

# The refresh secret defaults to the regular signing key so deployments that
# only configure SECRET_KEY keep working.
if not SERVER_REFRESH_SECRET:
    SERVER_REFRESH_SECRET = SECRET_KEY

# Mongo database name for users. Existing env files use the Node-style
# `DATABASE_COLLECTION_AUTH` value which may carry a leading slash (it used to
# be appended to the connection string), so normalize it here.
AUTH_DBNAME = str(DATABASE_COLLECTION_AUTH).lstrip("/") or "auth"


def parse_ttl(value) -> int:
    """Convert a TTL like '1d', '12h', '30m', '60s' or plain seconds to seconds."""
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip().lower()
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    if text and text[-1] in units:
        return int(float(text[:-1]) * units[text[-1]])
    return int(float(text))


SERVER_TTL_SECONDS = parse_ttl(SERVER_TTL)
SERVER_REFRESH_TTL_SECONDS = parse_ttl(SERVER_REFRESH_TTL)
