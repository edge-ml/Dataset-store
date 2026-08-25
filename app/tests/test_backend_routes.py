"""Tests for the routes migrated from the Node backend service.

Covers /api/projects, /api/devices, /api/deviceApi (key management) and
/api/arduinoFirmware as mounted in main.py.
"""
from bson.objectid import ObjectId

import pytest

from tests.conftest import fake_client, SECRET_KEY


@pytest.fixture
def devices_db():
    return fake_client["backend_test"]["devices"]


@pytest.fixture
def sensors_db():
    return fake_client["backend_test"]["sensors"]


# --------------------------------------------------------------------- #
# Projects
# --------------------------------------------------------------------- #
class TestProjects:
    def test_requires_token(self, client):
        r = client.get("/api/projects/")
        assert r.status_code == 401

    def test_invalid_token(self, client, seeder):
        r = client.get("/api/projects/", headers={"Authorization": "Bearer garbage"})
        assert r.status_code == 401

    def test_list_projects_returns_plain_ids(self, client, seeder):
        seeder.auth_user(seeder.admin_id, "admin")
        pid = seeder.project(users=[ObjectId()])
        r = client.get("/api/projects/", cookies={"jwt": seeder.token()})
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 1
        assert data[0]["_id"] == pid
        assert isinstance(data[0]["admin"], str)
        assert all(isinstance(u, str) for u in data[0]["users"])

    def test_non_admin_gets_filtered_fields(self, client, seeder):
        seeder.auth_user(seeder.admin_id)
        member = ObjectId()
        seeder.auth_user(member, "member")
        seeder.project(users=[member])
        r = client.get("/api/projects/",
                       cookies={"jwt": seeder.token(user_id=member)})
        assert r.status_code == 200
        project = r.json()[0]
        assert set(project.keys()) == {"name", "_id", "admin", "enableDeviceApi"}

    def test_project_with_unknown_users_drops_them(self, client, seeder):
        seeder.auth_user(seeder.admin_id)
        ghost = ObjectId()
        seeder.project(users=[ghost])
        # register the ghost's id as a real auth user first? No - it is unknown.
        r = client.get("/api/projects/", cookies={"jwt": seeder.token()})
        assert r.status_code == 200
        assert r.json()[0]["users"] == []

    def test_create_project(self, client, seeder):
        token = seeder.token()
        body = {"name": "my-project", "users": []}
        r = client.post("/api/projects/", json=body,
                        cookies={"jwt": token})
        assert r.status_code == 201
        assert r.json()["name"] == "my-project"
        assert str(seeder.admin_id) == r.json()["admin"]

    def test_create_duplicate_name_rejected(self, client, seeder):
        seeder.auth_user(seeder.admin_id)
        seeder.project()
        r = client.post("/api/projects/", json={"name": "test-project", "users": []},
                        cookies={"jwt": seeder.token()})
        assert r.status_code == 400
        assert "already exists" in r.json()["error"]

    def test_create_invalid_name_rejected(self, client, seeder):
        r = client.post("/api/projects/", json={"name": "bad name!", "users": []},
                        cookies={"jwt": seeder.token()})
        assert r.status_code == 400
        assert r.json()["error"] == "Invalid project name"

    def test_create_admin_cannot_be_user(self, client, seeder):
        token = seeder.token()
        admin_id = str(ObjectId())  # any id != admin would be fine; use admin to trigger
        payload = {"id": str(seeder.admin_id), "exp": _exp(3600)}
        import jwt as pyjwt
        token = pyjwt.encode(payload, SECRET_KEY, algorithm="HS256")
        r = client.post("/api/projects/", json={"name": "p2", "users": [str(seeder.admin_id)]},
                        cookies={"jwt": token})
        assert r.status_code == 400
        assert "Admin cannot be a user" in r.json()["error"]

    def test_delete_only_by_admin(self, client, seeder):
        seeder.auth_user(seeder.admin_id)
        pid = seeder.project()
        # non-members are rejected by authentication
        r = client.delete(f"/api/projects/{pid}",
                          headers={"project": pid},
                          cookies={"jwt": seeder.token(user_id=ObjectId())})
        assert r.status_code == 401

        # members who are not the admin cannot delete either
        other_project = ObjectId()
        member = ObjectId()
        fake_client["backend_test"]["projects"].insert_one({
            "_id": other_project,
            "name": "other-project",
            "admin": ObjectId(),
            "users": [member],
        })
        r = client.delete(f"/api/projects/{other_project}",
                          headers={"project": str(other_project)},
                          cookies={"jwt": seeder.token(user_id=member)})
        assert r.status_code == 400
        assert r.json()["message"] == "Cannot delete this project"

    def test_delete_by_admin(self, client, seeder):
        seeder.auth_user(seeder.admin_id)
        pid = seeder.project()
        r = client.delete(f"/api/projects/{pid}", headers={"project": pid},
                          cookies={"jwt": seeder.token()})
        assert r.status_code == 200, r.text
        assert fake_client["backend_test"]["projects"].find_one({"_id": ObjectId(pid)}) is None

    def test_leave_project(self, client, seeder):
        member = ObjectId()
        seeder.auth_user(member, "member")
        seeder.auth_user(seeder.admin_id)
        pid = seeder.project(users=[member])
        r = client.delete(f"/api/projects/{pid}/leave", headers={"project": pid},
                          cookies={"jwt": seeder.token(user_id=member)})
        assert r.status_code == 200
        doc = fake_client["backend_test"]["projects"].find_one({"_id": ObjectId(pid)})
        assert str(member) not in [str(u) for u in doc["users"]]

    def test_update_project_by_admin(self, client, seeder):
        seeder.auth_user(seeder.admin_id)
        pid = seeder.project()
        new_users = [str(ObjectId())]
        r = client.put(f"/api/projects/{pid}",
                       json={"name": "renamed", "users": new_users},
                       headers={"project": pid},
                       cookies={"jwt": seeder.token()})
        assert r.status_code == 200
        doc = fake_client["backend_test"]["projects"].find_one({"_id": ObjectId(pid)})
        assert doc["name"] == "renamed"
        assert [str(u) for u in doc["users"]] == new_users


def _exp(seconds):
    import datetime
    return datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=seconds)


# --------------------------------------------------------------------- #
# Devices
# --------------------------------------------------------------------- #
class TestDevices:
    def seed_device(self, devices_db, sensors_db):
        device_id = ObjectId()
        sensor_ids = [ObjectId(), ObjectId()]
        devices_db.insert_one({
            "_id": device_id,
            "name": "nicla",
            "generation": 1,
            "maxSampleRate": 200,
            "sensors": sensor_ids,
        })
        for sid in sensor_ids:
            sensors_db.insert_one({"_id": sid, "device": device_id, "name": f"s{sid}"})
        return device_id

    def test_get_device_truncates_generation(self, client, seeder, devices_db, sensors_db):
        device_id = self.seed_device(devices_db, sensors_db)
        r = client.get("/api/devices/nicla/1.2.3",
                       headers={"project": str(seeder.project())},
                       cookies={"jwt": seeder.token()})
        assert r.status_code == 200, r.text
        data = r.json()
        assert data["device"]["_id"] == str(device_id)
        assert len(data["sensors"]) == 2

    def test_get_device_not_found(self, client, seeder):
        r = client.get("/api/devices/nope/1",
                       headers={"project": str(seeder.project())},
                       cookies={"jwt": seeder.token()})
        assert r.status_code == 404
        assert r.json()["error"] == "Not found"


# --------------------------------------------------------------------- #
# DeviceApi key management
# --------------------------------------------------------------------- #
class TestDeviceApiKeys:
    def test_get_key_empty(self, client, seeder):
        r = client.get("/api/deviceApi/getKey",
                       headers={"project": str(seeder.project())},
                       cookies={"jwt": seeder.token()})
        assert r.status_code == 200
        assert r.json() == {"readApiKey": None, "writeApiKey": None}

    def test_set_key_stores_keys(self, client, seeder):
        headers = {"project": str(seeder.project())}
        r = client.get("/api/deviceApi/setKey", headers=headers,
                       cookies={"jwt": seeder.token()})
        assert r.status_code == 200
        keys = r.json()
        assert keys["readApiKey"] and keys["writeApiKey"]
        doc = seeder.deviceapis.find_one({"projectId": seeder.project_id})
        assert doc["readApiKey"] == keys["readApiKey"]
        assert doc["writeApiKey"] == keys["writeApiKey"]

    def test_set_key_rotates_existing(self, client, seeder):
        headers = {"project": str(seeder.project())}
        first = client.get("/api/deviceApi/setKey", headers=headers,
                           cookies={"jwt": seeder.token()}).json()
        second = client.get("/api/deviceApi/setKey", headers=headers,
                            cookies={"jwt": seeder.token()}).json()
        assert first["readApiKey"] != second["readApiKey"]

    def test_delete_key(self, client, seeder):
        headers = {"project": str(seeder.project())}
        client.get("/api/deviceApi/setKey", headers=headers, cookies={"jwt": seeder.token()})
        r = client.get("/api/deviceApi/deleteKey", headers=headers,
                       cookies={"jwt": seeder.token()})
        assert r.status_code == 200, r.text
        assert seeder.deviceapis.find_one({"projectId": seeder.project_id}) is None

    def test_switch_active_member_not_admin(self, client, seeder):
        member = ObjectId()
        pid = seeder.project(users=[member])
        r = client.post("/api/deviceApi/switchActive", json={"state": True},
                        headers={"project": pid},
                        cookies={"jwt": seeder.token(user_id=member)})
        assert r.status_code == 400

    def test_switch_active_outside_user(self, client, seeder):
        pid = seeder.project()
        outsider = ObjectId()
        r = client.post("/api/deviceApi/switchActive", json={"state": True},
                        headers={"project": pid},
                        cookies={"jwt": seeder.token(user_id=outsider)})
        # non-members are rejected already by authentication
        assert r.status_code == 401

    def test_switch_active_admin(self, client, seeder):
        pid = seeder.project()
        r = client.post("/api/deviceApi/switchActive", json={"state": True},
                        headers={"project": pid}, cookies={"jwt": seeder.token()})
        assert r.status_code == 200
        doc = fake_client["backend_test"]["projects"].find_one({"_id": ObjectId(pid)})
        assert doc["enableDeviceApi"] is True


# --------------------------------------------------------------------- #
# Arduino firmware
# --------------------------------------------------------------------- #
class TestArduinoFirmware:
    def test_unsupported_device(self, client, seeder):
        r = client.get("/api/arduinoFirmware/esp32",
                       cookies={"jwt": seeder.token()})
        assert r.status_code == 400
        assert "not supported" in r.json()["message"]

    def test_requires_auth(self, client):
        r = client.get("/api/arduinoFirmware/nicla")
        assert r.status_code == 401
