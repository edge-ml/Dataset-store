from starlette.config import Config

config = Config(".env")

SECRET_KEY = config("SECRET_KEY")
ENV = config("ENV")
MONGO_URI = config("MONGO_URI")
PROJECT_DBNAME = config("PROJECT_DBNAME")
PROJECT_COLLNAME = config("PROJECT_COLLNAME")
DATASTORE_DBNAME = config("DATASTORE_DBNAME")
DATASTORE_COLLNAME = config("DATASTORE_COLLNAME")
TIMESERIES_DBNAME = config("TIMESERIES_DBNAME")
TIMESERIES_COLLNAME = config("TIMESERIES_COLLNAME")