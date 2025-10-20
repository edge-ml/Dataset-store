import pytest
import rich


dataset_object = {
    "name": "Test Dataset",
    "timeSeries": [
        {
            "name": "TS1",
            "unit": "seconds",
            "data": [[0,1], [1,2], [2,3]]
        }
    ]
}



@pytest.mark.anyio
async def test_create_dataset(client):
    res = await client.post(
        "/api/v1/datasets/",
        json=dataset_object
    )
    rich.print(res.json())
    assert res.status_code == 201


@pytest.mark.anyio
async def test_dataset_pagination(client):
    res = await client.get(
        "/api/v1/datasets/view?skip=0&limit=2&sort=alphaAsc"
    )
    rich.print(res.json())
    assert res.status_code == 200