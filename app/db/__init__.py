from app.internal.config import DATABASE_URL, DATABASE_NAME
from beanie import init_beanie
from pymongo import AsyncMongoClient
import os

from app.features.Auth.models import AuthModel_DB
from app.features.Projects.models import ProjectModel_DB
from app.features.Datasets.models import DatasetModel_DB
from app.features.Labelings.models import LabelingModel_DB

db_models = [
    AuthModel_DB,
    ProjectModel_DB,
    DatasetModel_DB,
    LabelingModel_DB
    ]


DATABASE = None

async def init_db():
    global DATABASE
    if DATABASE is not None and not os.getenv("ENV") == "test":
        print("Database already initialized")
        return DATABASE
    assert DATABASE_URL is not None, "DATABASE_URL is not set"
    assert DATABASE_NAME is not None, "DATABASE_NAME is not set"
    client = AsyncMongoClient(
        DATABASE_URL,
        tz_aware=True)
    database = client[DATABASE_NAME]
    await init_beanie(database=database, document_models=db_models)
    DATABASE = database
    return database
