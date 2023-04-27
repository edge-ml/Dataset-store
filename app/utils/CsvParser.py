import numpy as np
from enum import Enum
import pandas as pd
import re

from app.utils.helpers import parseTime

class CSVFormat(Enum):
    EDGEML = "edgeml"
    STANDARD = "standard"

class CsvParser():
    def __init__(self, arr: bytearray, time=["time"], drop=[], format=CSVFormat.STANDARD) -> None:
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

    def _calcTime(self, x):
        pass

    @staticmethod
    def _calc_sensor_end_idx(header):
        sensor_end_idx = np.argwhere(np.char.startswith(header, 'label_'))
        # no label given or remaining after delete operation
        if np.size(sensor_end_idx) == 0:
            sensor_end_idx = len(header) + 1
        else:
            # take the first index
            sensor_end_idx = sensor_end_idx[0][0]
        return sensor_end_idx

    # TODO: add robustness checks to detect erroneous csv data
    def to_edge_ml_format(self, config: dict):
        line_data = self.arr.decode('utf-8').splitlines()
        str_data = [x.split(",") for x in line_data]
        ts_config = config['timeSeries']
        labeling_config = config['labelings']

        if len(str_data) < 2:
            print("no data")
            return None, None, None, None, None, None, None

        data = np.array(str_data)
        header = data[0]
        sensor_start_idx = 1
        sensor_end_idx = self._calc_sensor_end_idx(header)

        removed_timeseries = []
        removed_labelings = []
        for ts in ts_config:
            if ts['removed']:
                removed_timeseries.append(sensor_start_idx + ts['index'])
        for labeling in labeling_config:
            if labeling['removed']:
                removed_labelings.extend(
                    [sensor_end_idx + index for index in labeling['indices']])

        # remove unused columns to speed up
        data = np.delete(data, removed_timeseries + removed_labelings, axis=1)

        # update values after removal
        header = data[0]
        data = data[1:]
        sort_indices = np.argsort(data[:, 0].astype(np.uint64))
        sorted_data = data[sort_indices]
        time = sorted_data[:, 0]
        sensor_end_idx = self._calc_sensor_end_idx(header)

        # extract sensor data
        sensor_data = sorted_data[0:, 1:sensor_end_idx].T
        sensor_data = np.nan_to_num(sensor_data)

        # extract label data
        label_data = sorted_data[0:, sensor_end_idx:].T

        # extract names from file header
        sensor_names = header[1:sensor_end_idx]
        labeling_label_list = header[sensor_end_idx:]

        # remove 'sensor_' prefix
        sensor_names = [s[7:] for s in sensor_names]

        # extract units at the end from square brackets example: sensor_accX[unit]
        unit_pattern = r'\[([^\[\]]*)\]$'
        units = [
            re.search(unit_pattern, s).group(1)
            if re.search(unit_pattern, s) else '' for s in sensor_names
        ]

        # remove unit suffix from sensor names
        sensor_names = [re.sub(unit_pattern, '', s) for s in sensor_names]

        # modify names according to the config at the end (config has no 'sensor_' prefix and unit suffix)
        for i, sensor_name in enumerate(sensor_names):
            sensor_names[i] = next(x for x in ts_config
                                   if x['originalName'] == sensor_name)['name']

        # remove 'label_' prefix
        labeling_label_list = [l[6:] for l in labeling_label_list]
        labelings = {}

        for labeling_label in labeling_label_list:
            labeling, label = labeling_label.split('_')
            # modify labeling according to the config
            # TODO: implement the corresponding functionality in frontend to modify labelings?
            labeling = next(x for x in labeling_config
                            if x['originalName'] == labeling)['name']
            if labeling not in labelings:
                labelings[labeling] = []
            labelings[labeling].append(label)
        
        return time, sensor_data, label_data, sensor_names, labeling_label_list, labelings, units

    def _buffer_to_numpy(self, buf):
        line_data = self.arr.decode('utf-8').splitlines()
        str_data = [x.split(",") for x in line_data]
        if len(str_data) < 2: # Only got the header
            return None, None, str_data[0]

        df = pd.DataFrame(str_data[1:], columns=str_data[0])

        selected_time = None
        for t in self.time_col:
            if t in df.columns:
                selected_time = t
        if selected_time == None:
            raise Exception("No suitable time column has been found")

        df[selected_time] = df[selected_time].apply(parseTime)
        time_arr = df.loc[:, selected_time].to_numpy().astype(np.uint64)
        df = df.drop(self.drop_cols + [selected_time], axis=1, errors="ignore")
        data_arr = df.to_numpy().T.astype(np.float32)
        print(data_arr[:, 0])
        header = list(df.columns)
        return time_arr, data_arr, header

    def _convert_standard(self):
        return self._buffer_to_numpy(self.arr)
    
    def to_edge_ml(self):
        if self.format is CSVFormat.EDGEML:
            pass
        if self.format is CSVFormat.STANDARD:
            return self._convert_standard()
