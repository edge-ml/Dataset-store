import io

import numpy as np
import pandas as pd
import pytest

from utils.CsvParser import CSVFormat, CsvParser


CSV = (
    "time,sensor_temp[m/s],label_Activity_run\n"
    "100.500,1.0,x\n"
    "99.400,2.0,\n"          # unsorted on purpose
    "101.600,3.0,x\n"
)


def make_config(removed_sensor=False, removed_labeling=False):
    return {
        "name": "csvds",
        "timeSeries": [
            {"originalName": "temp", "originalUnit": "m/s", "name": "temp",
             "unit": "m/s", "removed": removed_sensor, "scale": 2.0, "offset": 1.0},
        ],
        "labelings": [
            {"originalName": "Activity", "name": "Activity",
             "removed": removed_labeling, "labels": ["run"]},
        ],
    }


class TestToEdgeMlFormat:
    def test_basic(self):
        parser = CsvParser(df=pd.read_csv(io.StringIO(CSV)))
        config = make_config()
        time, sensor_data, label_data, sensor_names, labeling_label_list, labelings, units = \
            parser.to_edge_ml_format(config)
        # sorted by time
        assert time == [99.4, 100.5, 101.6]
        assert list(sensor_data.columns) == ["sensor_temp[m/s]"]
        # scaling 2x + offset 1 applied
        assert sensor_data["sensor_temp[m/s]"].tolist() == [5.0, 3.0, 7.0]
        assert sensor_names == ["temp"]
        assert labeling_label_list == ["Activity_run"]
        assert labelings == {"Activity": ["run"]}
        assert units == ["m/s"]
        def is_set(v):
            return str(v).strip().lower() == "x"
        assert [is_set(v) for v in label_data[0]] == [False, True, True]

    def test_removed_columns_dropped(self):
        parser = CsvParser(df=pd.read_csv(io.StringIO(CSV)))
        time, sensor_data, label_data, *_ = parser.to_edge_ml_format(
            make_config(removed_sensor=True, removed_labeling=True))
        assert len(time) == 3
        assert sensor_data.shape[1] == 0
        assert len(label_data) == 0

    def test_flat_frame_returns_nones(self):
        df = pd.Series([1, 2])  # genuinely flat input (ndim < 2)
        parser = CsvParser(df=df)
        res = parser.to_edge_ml_format(make_config())
        assert all(r is None for r in res)

    def test_column_without_unit_suffix(self):
        csv = "time,sensor_t,label_A_r\n1.5,2,x\n"
        parser = CsvParser(df=pd.read_csv(io.StringIO(csv)))
        config = make_config()
        config["timeSeries"][0]["originalUnit"] = ""
        config["timeSeries"][0]["originalName"] = "t"
        config["labelings"].append({"originalName": "A", "name": "A",
                                    "removed": False, "labels": ["r"]})
        _, sensor_data, _, sensor_names, _, _, _ = parser.to_edge_ml_format(config)
        # names are re-mapped via the config (originalName -> new name)
        assert sensor_names == ["temp"]


class TestBufferToNumpy:
    def test_list_time_col(self):
        data = bytearray(b"time,t1,t2\n1690000100.5,1,2\n1690000200.0,3,4\n")
        parser = CsvParser(arr=data, time=["time", "timestamp"], drop=["t2"])
        t, d, header = parser.to_edge_ml()
        assert t.tolist() == [16900001005, 16900002000]
        assert header == ["t1"]
        assert d.tolist()[0] == [1.0, 3.0]

    def test_string_time_col(self):
        # regression: a plain-string default used to iterate characters
        data = bytearray(b"time,v\n1690000100.5,7\n")
        parser = CsvParser(arr=data, time="time")
        t, d, header = parser.to_edge_ml()
        assert t.tolist() == [16900001005]
        assert d.tolist() == [[7.0]]

    def test_header_only(self):
        parser = CsvParser(arr=bytearray(b"a,b\n"), time=["time"])
        t, d, header = parser.to_edge_ml()
        assert t is None and d is None and header == ["a", "b"]

    def test_trailing_newline(self):
        parser = CsvParser(arr=bytearray(b"time,v\n1690000100.5,7\n\n"), time=["time"])
        t, d, header = parser.to_edge_ml()
        assert t.tolist() == [16900001005]

    def test_no_time_col_raises(self):
        parser = CsvParser(arr=bytearray(b"a,b\n1,2\n"), time=["time"])
        with pytest.raises(Exception, match="No suitable time column"):
            parser.to_edge_ml()

    def test_invalid_timestamp_raises(self):
        parser = CsvParser(arr=bytearray(b"time,v\n12345678901,7\n"), time=["time"])
        with pytest.raises(ValueError, match="Timestamp is invalid"):
            parser.to_edge_ml()

    def test_seek_bounds(self):
        parser = CsvParser(arr=bytearray(b"abcdef"))
        parser.seek(3)
        assert parser.fp == 3
        parser.seek(-5)
        assert parser.fp == 3  # negative positions ignored
        parser.seek(1000)
        assert parser.fp == 3  # beyond end ignored


class TestFormats:
    def test_standard_format(self):
        parser = CsvParser(arr=bytearray(b"time,v\n1690000100.5,2\n"), format=CSVFormat.STANDARD)
        t, d, h = parser.to_edge_ml()
        assert h == ["v"]
        assert d.shape == (1, 1)

    def test_edgeml_format_returns_none(self):
        # EDGEML parsing is not implemented; documents current no-op behaviour
        parser = CsvParser(arr=bytearray(b"time,v\n1.0,2\n"), format=CSVFormat.EDGEML)
        assert parser.to_edge_ml() is None
