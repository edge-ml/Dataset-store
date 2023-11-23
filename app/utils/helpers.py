from bson.objectid import ObjectId
import numpy as np
import random

def custom_index(array, compare_function):
    for i, v in enumerate(array):
        if compare_function(v):
            return i


class PyObjectId(ObjectId):

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v, c):
        print(v)
        print(c)
        print("*"*50)
        if not ObjectId.is_valid(v):
            raise ValueError('Invalid objectid')
        return ObjectId(v)

    @classmethod
    def __get_pydantic_json_schema__(cls, field_schema):
        field_schema.update(type='string')


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