import pytest


@pytest.mark.anyio
async def test_create_labeling(client):
    res = await client.post(
        "/api/v1/labelings/",
        json={
            "name": "Test Labeling",
            "labels": [
                {"name": "Label1", "color": "#FF0000"},
                {"name": "Label2", "color": "#00FF00"}
            ]
        },
    )
    print(res.json())
    assert res.status_code == 201


@pytest.mark.anyio
async def test_get_labelings(client):
    # First, create a labeling
    res = await client.post(
        "/api/v1/labelings/",
        json={
            "name": "Test Labeling",
            "labels": [
                {"name": "Label1", "color": "#FF0000"},
                {"name": "Label2", "color": "#00FF00"}
            ]
        },
    )
    assert res.status_code == 201

    # Now, get the labelings
    res = await client.get("/api/v1/labelings/")
    print(res.json())
    assert res.status_code == 200
    assert len(res.json()) > 0