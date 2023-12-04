from fastapi.testclient import TestClient
from app.routers.dataset import validate_user
from main import app

import asyncio
import pytest


client = TestClient(app)

@pytest.mark.asyncio
async def test_getDatasets(mocker):
    future = asyncio.Future()
    future.set_result(-1)
    mocker.patch("app.routers.dataset.validate_user", return_value=future)

    res = client.get("/ds/datasets/", headers= {"project": "abc", "Authorization": "Bearer faketoken"})
    print(res.text)
    assert(res.status_code == 200)


# def test_createDatasets():
    # res = client.post("/ds/datasets/create")
    # print(res.text)
    # assert res.status_code == 200