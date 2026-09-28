import json

import numpy as np
import pytest
from bson.objectid import ObjectId

from utils.helpers import PyObjectId, custom_index, parseTime, random_hex_color
from utils.json_encoder import JSONEncoder


class TestCustomIndex:
    def test_found(self):
        assert custom_index([1, 2, 3], lambda x: x == 2) == 1

    def test_not_found_returns_none(self):
        assert custom_index([1, 2, 3], lambda x: x == 99) is None

    def test_empty(self):
        assert custom_index([], lambda x: True) is None


class TestPyObjectId:
    def test_valid(self):
        oid = str(ObjectId())
        assert PyObjectId.validate(oid, None) == ObjectId(oid)

    def test_invalid(self):
        with pytest.raises(ValueError):
            PyObjectId.validate("not-an-object-id", None)

    def test_json_schema(self):
        schema = {}
        PyObjectId.__get_pydantic_json_schema__(schema, None)
        assert schema == {"type": "string"}


class TestParseTime:
    def test_seconds(self):
        assert int(parseTime("1690000000.123")) == 1690000000123

    def test_milliseconds(self):
        assert int(parseTime("1690000000123.456")) == 1690000000123

    def test_invalid_length(self):
        with pytest.raises(ValueError):
            parseTime("123.456")

    def test_no_fraction_raises(self):
        # regression: used to silently return None
        with pytest.raises(ValueError):
            parseTime("1690000000")


class TestRandomHexColor:
    def test_format(self):
        c = random_hex_color()
        assert c.startswith("#")
        assert len(c) == 7
        int(c[1:], 16)


class TestJSONEncoder:
    def test_objectid(self):
        oid = ObjectId()
        assert json.loads(json.dumps({"a": oid}, cls=JSONEncoder))["a"] == str(oid)

    def test_ndarray(self):
        arr = np.array([1, 2, 3])
        assert json.loads(json.dumps({"a": arr}, cls=JSONEncoder))["a"] == [1, 2, 3]

    def test_fallback_raises(self):
        with pytest.raises(TypeError):
            json.dumps({"a": object()}, cls=JSONEncoder)
