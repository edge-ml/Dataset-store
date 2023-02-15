import numpy as np

class CsvParser():
    def __init__(self, arr: bytearray) -> None:
        self.arr = arr
        self.fp = 0
        self.data = None
        self.time = None

    def seek(self, pos):
        if pos < 0 or pos > len(self.arr):
            return
        self.fp = pos
    

    def to_labels(self):
        lines = [x.split(",") for x in self.readLines()]
        header = lines[0]
        labels = []

    def _calcTime(self, timestamp):
        t_split = str(timestamp).split(".")
        if len(t_split[0]) == 10:
            return np.array(t_split[0] + t_split[1][0:3]).astype(np.uint64)
        elif len(t_split[0]) == 13:
            return np.array(t_split[0]).astype(np.uint64)
        else:
            raise ValueError("Timestamp is invalid")

    def to_edge_ml_format(self):
        line_data = self.arr.decode('utf-8').splitlines()
        str_data = [x.split(",") for x in line_data]

        if len(str_data) < 2:
            print("no data")
            return None, None, None


        data = np.array(str_data)
        header = data[0]
        time = np.array([self._calcTime(x) for x in data[1:,0]]).astype(np.uint64)
        data = data[1:, 1:].astype(np.float32)

        data = np.nan_to_num(data)
        datas = []
        for i in range(data.shape[1]):
            datas.append(data[:, i])
        return time, datas, header[1:]