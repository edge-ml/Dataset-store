"""Sanity checks that the mongomock patch is active and supports the
MongoDB features the application relies on (e.g. positional '$' updates)."""
import pytest
from bson.objectid import ObjectId

from tests.conftest import fake_client


def test_patch_active(col=None):
    import pymongo
    from pymongo import MongoClient
    # managers receive the shared mongomock client, not a real pymongo client
    assert MongoClient("mongodb://whatever") is fake_client


def test_insert_and_find():
    col = fake_client["db"]["coll"]
    res = col.insert_one({"a": 1})
    assert col.find_one({"a": 1})["_id"] == res.inserted_id


def test_positional_update_operator():
    col = fake_client["db"]["coll"]
    ts_id = ObjectId()
    col.insert_one({"timeSeries": [{"_id": ts_id, "unit": ""}]})
    res = col.update_one({"timeSeries._id": ts_id},
                         {"$set": {"timeSeries.$.unit": "m/s"}})
    assert res.modified_count == 1
    assert col.find_one({})["timeSeries"][0]["unit"] == "m/s"


def test_reset_keeps_handles_valid():
    col = fake_client["db"]["coll"]
    col.insert_one({"x": 1})
    fake_client.reset()
    assert list(col.find({})) == []  # handle still usable, data cleared


def test_cursor_sort_skip_limit():
    col = fake_client["db"]["coll"]
    for name in ["c", "a", "b"]:
        col.insert_one({"name": name})
    got = [d["name"] for d in col.find({}).sort("name", 1).skip(1).limit(1)]
    assert got == ["b"]


def test_count_and_delete_many():
    col = fake_client["db"]["coll"]
    for i in range(4):
        col.insert_one({"i": i, "even": i % 2 == 0})
    assert col.count_documents({"even": True}) == 2
    assert col.delete_many({"even": True}).deleted_count == 2
