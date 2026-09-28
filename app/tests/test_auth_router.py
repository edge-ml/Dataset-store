"""Tests for the /auth routes implemented in the dataset-store."""
import datetime

import jwt as pyjwt
import pytest

from internal.config import SECRET_KEY, SERVER_REFRESH_SECRET
from tests.conftest import fake_client

USERS = "auth_test"


@pytest.fixture
def users():
    return fake_client[USERS]["users"]


def bearer(user_id):
    payload = {
        "id": str(user_id),
        "email": "tester@edge-ml.com",
        "userName": "tester",
        "subscriptionLevel": "standard",
        "exp": datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=3600),
    }
    return {"Authorization": f"Bearer {pyjwt.encode(payload, SECRET_KEY, algorithm='HS256')}"}


def seed_user(users, **overrides):
    from bson.objectid import ObjectId
    doc = {
        "_id": ObjectId(),
        "email": overrides.get("email", "tester@edge-ml.com"),
        "userName": overrides.get("userName", "tester"),
        "password": overrides.get("password", "$2a$10$notarealhash"),
        "role": overrides.get("role", "user"),
        "refreshToken": overrides.get("refreshToken"),
        "subscriptionLevel": "standard",
    }
    users.insert_one(doc)
    return doc


class TestRegister:
    def test_register_success(self, client, users):
        r = client.post("/auth/register", json={
            "password": "supersecret1", "userName": "newbie",
        })
        assert r.status_code == 201
        assert r.json() == {"message": "Successfully created user!"}
        doc = users.find_one({"userName": "newbie"})
        assert "email" not in doc
        assert doc["password"].startswith("$2")  # bcrypt hash
        assert doc["role"] == "user"

    def test_register_without_email_twice(self, client, users):
        for name in ("first", "second"):
            r = client.post("/auth/register", json={"password": "supersecret1", "userName": name})
            assert r.status_code == 201

    def test_register_missing_username(self, client):
        r = client.post("/auth/register", json={"password": "supersecret1", "userName": "  "})
        assert r.status_code == 400

    def test_register_short_password(self, client):
        r = client.post("/auth/register", json={
            "password": "short", "userName": "x",
        })
        assert r.status_code == 400

    def test_register_duplicate_username(self, client, users):
        seed_user(users)
        r = client.post("/auth/register", json={
            "password": "supersecret1", "userName": "tester",
        })
        assert r.status_code == 409
        assert "username is already taken" in r.json()["detail"]


class TestLoginRefresh:
    def _seed_with_password(self, users):
        from controller.auth_controller import hash_password
        return seed_user(users, password=hash_password("correct-horse"))

    def test_login_by_username(self, client, users):
        self._seed_with_password(users)
        r = client.post("/auth/login", json={"userName": "tester", "password": "correct-horse"})
        assert r.status_code == 200
        body = r.json()
        decoded = pyjwt.decode(body["access_token"], SECRET_KEY, algorithms=["HS256"])
        assert decoded["userName"] == "tester"
        # refresh token must verify with its own secret
        pyjwt.decode(body["refresh_token"], SERVER_REFRESH_SECRET, algorithms=["HS256"])

    def test_login_legacy_email_field(self, client, users):
        self._seed_with_password(users)
        r = client.post("/auth/login", json={"email": "tester", "password": "correct-horse"})
        assert r.status_code == 200

    def test_login_by_email_rejected(self, client, users):
        self._seed_with_password(users)
        r = client.post("/auth/login", json={"userName": "tester@edge-ml.com", "password": "correct-horse"})
        assert r.status_code == 404

    def test_login_trims_username(self, client, users):
        self._seed_with_password(users)
        r = client.post("/auth/login", json={"userName": " tester ", "password": "correct-horse"})
        assert r.status_code == 200

    def test_login_legacy_padded_username(self, client, users):
        from controller.auth_controller import hash_password
        seed_user(users, userName=" padded ", password=hash_password("correct-horse"))
        r = client.post("/auth/login", json={"userName": "padded", "password": "correct-horse"})
        assert r.status_code == 200

    def test_login_wrong_password(self, client, users):
        self._seed_with_password(users)
        r = client.post("/auth/login", json={"userName": "tester", "password": "wrong"})
        assert r.status_code == 404

    def test_login_unknown_user(self, client):
        r = client.post("/auth/login", json={"userName": "ghost", "password": "x"})
        assert r.status_code == 404


class TestUser:
    def test_get_user(self, client, users):
        doc = seed_user(users)
        r = client.get("/auth/user", headers=bearer(doc["_id"]))
        assert r.status_code == 200
        body = r.json()
        assert body["email"] == "tester@edge-ml.com"
        assert "password" not in body and "refreshToken" not in body

    def test_get_user_unauthorized(self, client):
        assert client.get("/auth/user").status_code == 401


class TestUserManagement:
    def test_unregister_requires_matching_username(self, client, users):
        doc = seed_user(users)
        r = client.request("DELETE", "/auth/unregister", headers=bearer(doc["_id"]), json={"userName": "wrong"})
        assert r.status_code == 400

    def test_unregister(self, client, users):
        doc = seed_user(users)
        r = client.request("DELETE", "/auth/unregister", headers=bearer(doc["_id"]),
                           json={"userName": "tester"})
        assert r.status_code == 200
        assert users.find_one({"_id": doc["_id"]}) is None

    def test_change_username(self, client, users):
        doc = seed_user(users)
        r = client.put("/auth/changeUserName", headers=bearer(doc["_id"]), json={"userName": "renamed"})
        assert r.status_code == 200
        assert users.find_one({"_id": doc["_id"]})["userName"] == "renamed"

    def test_change_password(self, client, users):
        from controller.auth_controller import hash_password, check_password
        doc = seed_user(users, password=hash_password("old-pass"))
        r = client.put("/auth/changePassword", headers=bearer(doc["_id"]),
                       json={"password": "old-pass", "newPassword": "brand-new-pass"})
        assert r.status_code == 200
        assert check_password("brand-new-pass", users.find_one({"_id": doc["_id"]})["password"])

    def test_change_password_wrong_current(self, client, users):
        from controller.auth_controller import hash_password
        doc = seed_user(users, password=hash_password("old-pass"))
        r = client.put("/auth/changePassword", headers=bearer(doc["_id"]),
                       json={"password": "wrong", "newPassword": "x"})
        assert r.status_code == 400


class TestMappings:
    def test_ids_and_names(self, client, users):
        doc = seed_user(users)

        r = client.post("/auth/id", headers=bearer(doc["_id"]), json=["tester"])
        assert r.status_code == 200
        assert r.json() == [{"_id": str(doc["_id"]), "userName": "tester"}]

        r = client.post("/auth/userName", headers=bearer(doc["_id"]), json=[str(doc["_id"])])
        assert r.status_code == 200
        assert r.json() == [{"_id": str(doc["_id"]), "userName": "tester"}]

    def test_missing_name(self, client, users):
        doc = seed_user(users)
        r = client.post("/auth/id", headers=bearer(doc["_id"]), json=["ghost"])
        assert r.status_code == 400

    def test_suggest(self, client, users):
        seed_user(users, userName="alice")
        seed_user(users, email="a2@edge-ml.com", userName="alice_2")
        me = seed_user(users, email="me@edge-ml.com", userName="bob")
        r = client.post("/auth/userNameSuggest", headers=bearer(me["_id"]), json={"userName": "ali"})
        assert r.status_code == 200
        assert sorted(r.json()) == ["alice", "alice_2"]
