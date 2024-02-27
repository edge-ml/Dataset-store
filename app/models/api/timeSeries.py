from pydantic import RootModel
from typing import List, Tuple


class TimeSeries(RootModel):
    root: List[Tuple[int, float]]

    model_config = {
        "json_schema_extra": {
            "examples": [
                [["1709027288763", "3.41"],
                ["1709027298763", "5.64"],
                ["1709027318763", "3.32"]]
            ]
        }
    }