# Consts
import os
from os.path import join
import struct
import numpy as np
from scipy.signal import resample
import lttbc


DATA_PREFIX = "DATA"

cache = {}

class BinaryStore():

    def __init__(self, _id) -> None:
        self._id = str(_id)
        self.time_arr = np.array([], dtype=np.uint32)
        self.data_arr = np.array([], dtype=np.float32)

        self._path = join(DATA_PREFIX, self._id + ".bin")

    def loadSeries(self):
        if self._id not  in cache:
            with open(self._path, "rb") as f:
                len = struct.unpack("I", f.read(4))[0]
                self.time_arr = np.asarray(struct.unpack("I" * len, f.read(len * 4)))
                self.data_arr = np.asarray(struct.unpack("f" * len, f.read(len * 4)))
            cache[self._id] = self
        else:
            self.time_arr = cache[self._id].time_arr
            self.data_arr = cache[self._id].data_arr


    def saveSeries(self):
        with open(join(DATA_PREFIX, self._id + ".bin"), "wb") as f:
            f.write(struct.pack("I", len(self.time_arr)))
            f.write(struct.pack("I" * len(self.time_arr), *self.time_arr))
            f.write(struct.pack("f" * len(self.data_arr), *self.data_arr))

    def getPart(self, start_time, end_time, max_resolution=None):
        max_resolution = int(float(max_resolution))

        start_index = 0
        end_index = len(self.time_arr) -1
        if start_time != "undefined" and end_time != "undefined":
            end_time = int(end_time)
            start_time = int(start_time)
            [start_index, end_index] = np.searchsorted(self.time_arr, [start_time, end_time])
        time_res = self.time_arr[start_index:end_index]
        data_res = self.data_arr[start_index:end_index]
        if max_resolution is not None and len(time_res) > max_resolution:
            [time_res, data_res] = lttbc.downsample(time_res, data_res, max_resolution)
        res = np.asarray([time_res, data_res]).T
        res = np.ascontiguousarray(res)
        return res



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