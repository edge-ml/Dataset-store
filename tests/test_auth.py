import pytest

@pytest.mark.anyio
async def test_register(client_no_auth):
    res = await client_no_auth.post(
        "/api/v1/auth/register",
        json={
            "email": "testuser@example.com",
            "username": "testuser",
            "password": "testpassword"
        }
    )
    print(res.json())
    assert res.status_code == 201


@pytest.mark.anyio
async def test_login(client_no_auth):
    res = await client_no_auth.post(
        "/api/v1/auth/register",
        json={
            "email": "testuser@example.com",
            "username": "testuser",
            "password": "testpassword"
        }
    )
    print(res.json())
    assert res.status_code == 201

    # Now login
    res = await client_no_auth.post(
        "/api/v1/auth/token",
        data={
            "username": "testuser",
            "password": "testpassword"
        }
    )
    assert res.status_code == 200