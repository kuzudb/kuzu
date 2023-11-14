import sys
import numpy as np
import pandas as pd
import datetime
import pytest
import re

sys.path.append('../build/')
import kuzu


def test_scan_pandas(get_tmp_path):
    db = kuzu.Database(get_tmp_path)
    conn = kuzu.Connection(db)
    data = {
        'BOOL': [True, False, True, False],
        'UINT8': np.array([1, 2, 3, 4], dtype=np.uint8),
        'UINT16': np.array([10, 20, 30, 40], dtype=np.uint16),
        'UINT32': np.array([100, 200, 300, 400], dtype=np.uint32),
        'UINT64': np.array([1000, 2000, 3000, 4000], dtype=np.uint64),
        'INT8': np.array([-1, -2, -3, -4], dtype=np.int8),
        'INT16': np.array([-10, -20, -30, -40], dtype=np.int16),
        'INT32': np.array([-100, -200, -300, -400], dtype=np.int32),
        'INT64': np.array([-1000, -2000, -3000, -4000], dtype=np.int64),
        'FLOAT_32': np.array([-0.5199999809265137, float("nan"), -3.299999952316284, 4.400000095367432],
                             dtype=np.float32),
        'FLOAT_64': np.array([5132.12321, 24.222, float("nan"), 4.444], dtype=np.float64),
        'datetime_microseconds': np.array([
            np.datetime64('1996-04-01T12:00:11.500001'),
            np.datetime64('1981-11-13T22:02:52.000002'),
            np.datetime64('1972-12-21T12:05:44.500003'),
            np.datetime64('2008-01-11T22:10:03.000004')
        ]).astype('datetime64[us]'),
        'timedelta_nanoseconds': [
            np.timedelta64(500000, 'ns'),
            np.timedelta64(1000000000, 'ns'),
            np.timedelta64(2500000000, 'ns'),
            np.timedelta64(3022000000, 'ns')
        ]
    }
    df = pd.DataFrame(data)
    results = conn.execute("CALL READ_PANDAS('df') RETURN *")
    assert results.get_next() == [True, 1, 10, 100, 1000, -1, -10, -100, -1000, -0.5199999809265137, 5132.12321,
                                  datetime.datetime(1996, 4, 1, 12, 0, 11, 500001),
                                  datetime.timedelta(microseconds=500)]
    assert results.get_next() == [False, 2, 20, 200, 2000, -2, -20, -200, -2000, None, 24.222,
                                  datetime.datetime(1981, 11, 13, 22, 2, 52, 2),
                                  datetime.timedelta(seconds=1)]
    assert results.get_next() == [True, 3, 30, 300, 3000, -3, -30, -300, -3000, -3.299999952316284, None,
                                  datetime.datetime(1972, 12, 21, 12, 5, 44, 500003),
                                  datetime.timedelta(seconds=2, milliseconds=500)]
    assert results.get_next() == [False, 4, 40, 400, 4000, -4, -40, -400, -4000, 4.400000095367432, 4.444,
                                  datetime.datetime(2008, 1, 11, 22, 10, 3, 4),
                                  datetime.timedelta(seconds=3, milliseconds=22)]


def test_scan_pandas_with_filter(get_tmp_path):
    db = kuzu.Database(get_tmp_path)
    conn = kuzu.Connection(db)
    data = {
        'id': np.array([22, 3, 100], dtype=np.uint8),
        'weight': np.array([23.2, 31.7, 42.9], dtype=np.float64)
    }
    df = pd.DataFrame(data)
    results = conn.execute("CALL READ_PANDAS('df') WHERE id > 20 RETURN id + 5, weight")
    assert results.get_next() == [27, 23.2]
    assert results.get_next() == [105, 42.9]


def test_scan_invalid_pandas(get_tmp_path):
    db = kuzu.Database(get_tmp_path)
    conn = kuzu.Connection(db)
    with pytest.raises(RuntimeError,
                       match=re.escape("Binder exception: Cannot match a built-in function for given function "
                                       "READ_PANDAS(STRING). Supported inputs are\n(POINTER)\n")):
        conn.execute("CALL READ_PANDAS('df213') WHERE id > 20 RETURN id + 5, weight")
