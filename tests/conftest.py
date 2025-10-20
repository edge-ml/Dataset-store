import asyncio
import os  # nopep8
os.environ["ENV"] = "test"  # nopep8
from httpx import ASGITransport, AsyncClient
from motor.motor_asyncio import AsyncIOMotorClient

import pytest
from app.db import init_db
from app.internal.config import DATABASE_URL, DATABASE_NAME
from main import app

@pytest.fixture
def anyio_backend():
    return 'asyncio'

@pytest.fixture(autouse=True, scope="function")
async def init_database():
    await init_db()
    client = AsyncIOMotorClient(DATABASE_URL)
    db = client[DATABASE_NAME]
    for collection_name in await db.list_collection_names():
        await db.drop_collection(collection_name)
    yield


@pytest.fixture
async def client_no_auth():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test", follow_redirects=True) as client:
        yield client


@pytest.fixture
async def client_no_project(client_no_auth):
    # Register a user
    res = await client_no_auth.post(
        "/api/v1/auth/register",
        json={
            "email": "testuser@example.com",
            "username": "testuser",
            "password": "testpassword"
        }
    )
    assert res.status_code == 201

    res = await client_no_auth.post(
        "/api/v1/auth/token",
        data={
            "username": "testuser",
            "password": "testpassword"
        }
    )
    assert res.status_code == 200
    return client_no_auth


@pytest.fixture
async def client(client_no_project):
    # Create a project
    res = await client_no_project.post(
        "/api/v1/projects/",
        json={
            "name": "Test Project"
        }
    )
    assert res.status_code == 201
    client_no_project.headers["project"] = res.json()["id"]
    return client_no_project
