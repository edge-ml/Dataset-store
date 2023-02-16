import numpy as np
from enum import Enum
import pandas as pd

from app.utils.helpers import parseTime

class CSVFormat(Enum):
    EDGEML = "edgeml"
    STANDARD = "standard"

class CsvParser():
    def __init__(self, arr: bytearray, time="time", drop=[], format=CSVFormat.STANDARD) -> None:
        self.arr = arr
        self.fp = 0
        self.data = None
        self.time = None
        self.time_col = time
        self.drop_cols = drop
        self.format = format

    def seek(self, pos):
        if pos < 0 or pos > len(self.arr):
            return
        self.fp = pos
    

    def to_labels(self):
        lines = [x.split(",") for x in self.readLines()]
        header = lines[0]
        labels = []

    # def to_edge_ml_format(self):
    #     line_data = self.arr.decode('utf-8').splitlines()
    #     str_data = [x.split(",") for x in line_data]

    #     if len(str_data) < 2:
    #         print("no data")
    #         return None, None, None


    #     data = np.array(str_data)
    #     header = data[0]
    #     time = np.array([self._calcTime(x) for x in data[1:,0]]).astype(np.uint64)
    #     data = data[1:, 1:].astype(np.float32)

    #     data = np.nan_to_num(data)
    #     datas = []
    #     for i in range(data.shape[1]):
    #         datas.append(data[:, i])
    #     return time, datas, header[1:]

    def _buffer_to_numpy(self, buf):
        line_data = self.arr.decode('utf-8').splitlines()
        str_data = [x.split(",") for x in line_data]
        if len(str_data) < 2: # Only got the header
            return None, None, str_data[0]

        df = pd.DataFrame(str_data[1:], columns=str_data[0])
        df[self.time_col] = df[self.time_col].apply(parseTime)
        time_arr = df.loc[:, self.time_col].to_numpy().astype(np.uint64)
        df = df.drop(self.drop_cols + [self.time_col], axis=1, errors="ignore")
        data_arr = df.to_numpy().T
        header = list(df.columns)
        return time_arr, data_arr, header

    def _convert_standard(self):
        return self._buffer_to_numpy(self.arr)
        


    def to_edge_ml(self):
        if self.format is CSVFormat.EDGEML:
            pass
        if self.format is CSVFormat.STANDARD:
            return self._convert_standard()
