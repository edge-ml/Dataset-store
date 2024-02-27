from bson.objectid import ObjectId
import numpy as np
import random
from typing import Any
from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema
from pydantic.json_schema import JsonSchemaValue

def custom_index(array, compare_function):
    for i, v in enumerate(array):
        if compare_function(v):
            return i


# class PyObjectId(ObjectId):

#     @classmethod
#     def __get_validators__(cls):
#         yield cls.validate

#     @classmethod
#     def validate(cls, v, c):
#         if not ObjectId.is_valid(v):
#             raise ValueError('Invalid objectid')
#         return PyObjectId(v)

#     @classmethod
#     def __get_pydantic_json_schema__(cls, field_schema, context):
#         field_schema.update(type='string')
#         return {}
    
#     @classmethod
#     def __get_pydantic_field__(cls, *args, **kwargs):
#         return (str, ...), kwargs

class PyObjectId:
    @classmethod
    def validate_object_id(cls, v: Any, handler) -> ObjectId:
        if isinstance(v, ObjectId):
            return v

        s = handler(v)
        if ObjectId.is_valid(s):
            return ObjectId(s)
        else:
            raise ValueError("Invalid ObjectId")

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type, _handler) -> core_schema.CoreSchema:
        return core_schema.no_info_wrap_validator_function(
            cls.validate_object_id, 
            core_schema.str_schema(), 
            serialization=core_schema.to_string_ser_schema(),
        )

    @classmethod
    def __get_pydantic_json_schema__(cls, _core_schema, handler) -> JsonSchemaValue:
        return handler(core_schema.str_schema())
        






def parseTime(timestamp):
    if "." not in timestamp:
        return 
    t_split = str(timestamp).split(".")
    if len(t_split[0]) == 10:
        return np.array(t_split[0] + t_split[1][0:3]).astype(np.uint64)
    elif len(t_split[0]) == 13:
        return np.array(t_split[0]).astype(np.uint64)
    else:
        raise ValueError("Timestamp is invalid")
    
def random_hex_color():
    # generate a random integer between 0 and 16777215 (FFFFFF in hexadecimal)
    color = random.randint(0, 16777215)
    # convert the integer to a 6-digit hexadecimal string and return it
    return '#' + hex(color)[2:].upper().zfill(6)