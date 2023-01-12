# Consts
import os
from os.path import join
import struct
import numpy as np



DATA_PREFIX = "DATA"



class BinaryStore():

    def __init__(self, _id) -> None:
        self._id = str(_id)
        self.time_arr = np.array([], dtype=np.uint32)
        self.data_arr = np.array([], dtype=np.float32)

        self._path = join(DATA_PREFIX, self._id + ".bin")

    def loadSeries(self):
        with open(self._path, "rb") as f:
            len = struct.unpack("I", f.read(4))[0]
            self.time_arr = np.asarray(struct.unpack("I" * len, f.read(len * 4)))
            self.data_arr = np.asarray(struct.unpack("f" * len, f.read(len * 4)))


    def saveSeries(self):
        with open(join(DATA_PREFIX, self._id + ".bin"), "wb") as f:
            f.write(struct.pack("I", len(self.time_arr)))
            f.write(struct.pack("I" * len(self.time_arr), *self.time_arr))
            f.write(struct.pack("f" * len(self.data_arr), *self.data_arr))

    def getPart(self, start_time, end_time):
        # start_index = min([i for (i, d) in enumerate(self.time_arr) if d >= start_time])
        # end_idx = max([i for (i, d) in enumerate(self.time_arr) if d <= end_time])

        [start_index, end_index] = np.searchsorted(self.time_arr, [start_time, end_time])

        return {"time": self.time_arr[start_index:end_index].tolist(), "data": self.data_arr[start_index:end_index].tolist()}

    def getFull(self):
        return {"time": self.time_arr, "data": self.data_arr}

    def append(self, tsValues):

        time, data = list(zip(*tsValues))
        time, data = list(time), list(data)

        time = np.array(time, dtype=np.uint32)
        data = np.array(data, dtype=np.float32)
        self.time_arr = np.append(self.time_arr, time)
        self.data_arr = np.append(self.data_arr, data)

        inds = self.time_arr.argsort()
        self.time_arr = self.time_arr[inds]
        self.data_arr = self.data_arr[inds]
        self.saveSeries()
        return self.time_arr[0], self.time_arr[-1] # Return start and end


    def delete(self):
        os.remove(self._path)