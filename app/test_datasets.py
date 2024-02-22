from fastapi import FastAPI
from fastapi.testclient import TestClient
from main import app
import unittest
from unittest.mock import patch
from bson.objectid import ObjectId
from utils.helpers import PyObjectId
from db.project import ProjectDBManager

client = TestClient(app)

headers = {
    "Authorization": "Bearer fake_token",
    "project": str(PyObjectId())
}

fake_jwt_decoded = {
    "mail": "fake@teco.edu",
    "userName": "fake_user_name",
    "id": str(PyObjectId()),
    "subscriptionLevel": "standard",
    "iat": "0",
    "exp": "2147483647"
}

fake_project = {
    "admin": fake_jwt_decoded["id"],
    "users": []
}


class TestDatasts(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_jwt_decode = patch("routers.dependencies.decode").start()
        self.mock_jwt_decode.return_value = fake_jwt_decoded
        self.mock_get_project = patch("routers.dependencies.project_dbm.get_project").start()
        self.mock_get_project.return_value = fake_project

    def tearDown(self) -> None:
        patch.stopall()

    def fake_jwt_validator(self, token: str):
        if token == headers["Authorization"]:
            return True
        return False


    def test_get_datasets(self):
        response = client.get("/ds/datasets", headers=headers)
        print(response.json())
        assert response.status_code == 200

    # def test_create_dataset(self):

