import pytest

@pytest.mark.anyio
async def test_create_project(client_no_project):
    res = await client_no_project.post(
        "/api/v1/projects/",
        json={
            "name": "Test Project"
        }
    )
    print(res.json())
    assert res.status_code == 201
    assert res.json()["name"] == "Test Project"