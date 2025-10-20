from starlette.config import Config
import os

default_values = {
    "S3_URL": None,
    "S3_BUCKET_NAME": None,
    "S3_ACCESS_KEY": None,
    "S3_SECRET_KEY": None,
    "S3_TIMEOUT_SECONDS": 2 # Default to 5 seconds, as a compromise
}

config =  lambda x: None

ENV_FILE = ".env"
if os.getenv("ENV") == "test":
    print("Test environment detected")
    ENV_FILE = ".env.test"

config = Config(ENV_FILE)
for (k, v) in config.file_values.items():
    globals()[k] = v

for variable, default_value in default_values.items():
    if variable not in globals():
        globals()[variable] = default_value

DATABASE_URL = config("DATABASE_URL", default=None)
DATABASE_NAME = config("DATABASE_NAME", default=None)


SECRET_KEY = config("SECRET_KEY", default=None)
ACCESS_TOKEN_EXPIRE_MINUTES = config("ACCESS_TOKEN_EXPIRE_MINUTES", cast=int, default=None)
REFRESH_TOKEN_EXPIRE_DAYS = config("REFRESH_TOKEN_EXPIRE_DAYS", cast=int, default=None)


assert DATABASE_URL is not None, "DATABASE_URL is not set"
assert DATABASE_NAME is not None, "DATABASE_NAME is not set"

assert SECRET_KEY is not None, "SECRET_KEY is not set"
assert ACCESS_TOKEN_EXPIRE_MINUTES is not None, "ACCESS_TOKEN_EXPIRE_MINUTES is not set"
assert REFRESH_TOKEN_EXPIRE_DAYS is not None, "REFRESH_TOKEN_EXPIRE_DAYS is not set"