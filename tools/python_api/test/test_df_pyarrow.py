import pandas as pd, pyarrow as pa
import pytz
import struct, random, math
from datetime import datetime, timedelta
from pandas.arrays import ArrowExtensionArray as arrowtopd
import pytest
import kuzu
from type_aliases import ConnDB
from pathlib import Path

def generate_primitive(dtype):
    if (random.randrange(0, 5) == 0):
        return None
    if (dtype.startswith("bool")):
        return random.randrange(0, 1) == 1
    if (dtype.startswith("int32")):
        return random.randrange(-2147483648, 2147483647)
    if (dtype.startswith("int64")):
        return random.randrange(-9223372036854775808, 9223372036854775807)
    if (dtype.startswith("uint64")):
        return random.randrange(0, 18446744073709551615)
    if (dtype.startswith("float32")):
        random_bits = random.getrandbits(32)
        random_bytes = struct.pack('<I', random_bits)
        random_float = struct.unpack('<f', random_bytes)[0]
        return random_float
    return -1

def generate_primitive_series(scale, dtype):
    return pd.Series([generate_primitive(dtype) for i in range(scale)], dtype=dtype)

def generate_primitive_df(scale, names, schema):
    return pd.DataFrame({name:generate_primitive_series(scale, dtype) for name, dtype in zip(names, schema)})

def set_thread_count(conn, cnt):
    conn.execute(f"CALL THREADS={cnt}")

def tables_equal(t1, t2):
    if t1.schema != t2.schema:
        return False
    if t1.num_rows != t2.num_rows:
        return False
    for col_name in t1.schema.names:
        col1 = t1[col_name]
        col2 = t2[col_name]
        if col1 != col2:
            return False
    return True

def is_null(val):
    if val is None:
        return True
    if type(val) is str:
        return val == ''
    if type(val) is pd._libs.missing.NAType:
        return True
    if type(val) is float:
        return math.isnan(val)
    return False

def pyarrow_test_helper(establish_connection, n, k):
    conn, db = establish_connection
    names = [
        'boolcol', 'int32col', 'int64col', 'uint64col', 'floatcol'
    ]
    schema = [
        'bool[pyarrow]', 'int32[pyarrow]', 'int64[pyarrow]', 'uint64[pyarrow]', 'float32[pyarrow]'
    ]
    set_thread_count(conn, k)
    random.seed(n * k)
    df = generate_primitive_df(n, names, schema).sort_values(by=['int32col', 'int64col', 'uint64col', 'floatcol'])
    patable = pa.Table.from_pandas(df).select(names)
    result = conn.execute(
        'CALL READ_PANDAS(df) RETURN boolcol, int32col, int64col, uint64col, floatcol ORDER BY int32col, int64col, uint64col, floatcol'
    ).get_as_arrow(n)
    if (not tables_equal(patable, result)):
        print(patable)
        print('-'*25)
        print(result)
        print('-'*25)
        pytest.fail('tables are not equal')

def test_pyarrow_primitive(tmp_path : Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    establish_connection = (conn, db)
    # stress tests primitive reading
    sfs = [100, 2048, 4000, 9000, 16000]
    threads = [1, 2, 5, 10]
    for sf in sfs:
        for thread in threads:
            pyarrow_test_helper(establish_connection, sf, thread)

def test_pyarrow_time(conn_db_readonly : ConnDB) -> None:
    conn, db = conn_db_readonly
    col1 = pa.array([1000123, 2000123, 3000123], type=pa.duration('s'))
    col2 = pa.array([1000123000000, 2000123000000, 3000123000000], type=pa.duration('us'))
    col3 = pa.array([1000123000000000, 2000123000000000, 3000123000000000], type=pa.duration('ns'))
    col4 = pa.array([datetime(2012, 1, 20), datetime(2000, 12, 2), datetime(1987, 5, 27)],
        type=pa.timestamp('s'))
    col5 = pa.array([datetime(2012, 1, 20), datetime(2000, 12, 2), datetime(1987, 5, 27)],
        type=pa.timestamp('s'))
    col6 = pa.array([datetime(2012, 1, 20), datetime(2000, 12, 2), datetime(1987, 5, 27)],
        type=pa.timestamp('ms'))
    col7 = pa.array([datetime(2012, 1, 20), datetime(2000, 12, 2), datetime(1987, 5, 27)],
        type=pa.timestamp('us'))
    col8 = pa.array([datetime(2012, 1, 20), datetime(2000, 12, 2), datetime(1987, 5, 27)],
        type=pa.timestamp('ns'))
    col9 = pa.array([datetime(2012, 1, 20), datetime(2000, 12, 2), datetime(1987, 5, 27)],
        type=pa.date32())
    col10 = pa.array([datetime(2012, 1, 20), datetime(2000, 12, 2), datetime(1987, 5, 27)],
        type=pa.date64())
    # not implemented by pandas
    # col11 = pa.array([(1, 2, 3), (4, 5, -6), (100, 200, 1000000000)], type=pa.month_day_nano_interval())
    # for some reason, pyarrow doesnt support the direct creation of pure month or pure datetime
    # intervals, so that will remain untested for now
    df = pd.DataFrame({
        'col1': arrowtopd(col1),
        'col2': arrowtopd(col2),
        'col3': arrowtopd(col3),
        'col4': arrowtopd(col4),
        'col5': arrowtopd(col5),
        'col6': arrowtopd(col6),
        'col7': arrowtopd(col7),
        'col8': arrowtopd(col8),
        'col9': arrowtopd(col9),
        'col10': arrowtopd(col10)
        #'col11': arrowtopd(col11)
    })
    result = conn.execute('CALL READ_PANDAS(df) RETURN *').get_as_df()
    for colname in ['col1', 'col2', 'col3']:
        for expected, actual in zip(df[colname], result[colname]):
            tmp1 = expected if type(expected) is timedelta else expected.to_pytimedelta()
            tmp2 = actual if type(actual) is timedelta else actual.to_pytimedelta()
            assert tmp1 == tmp2
    for colname in ['col4', 'col5', 'col6', 'col7', 'col8']:
        for expected, actual in zip(df[colname], result[colname]):
            tmp1 = expected if type(expected) is datetime else expected.to_pydatetime()
            tmp2 = actual if type(actual) is datetime else actual.to_pydatetime()
            assert tmp1 == tmp2
    for colname in ['col9', 'col10']:
        for expected, actual in zip(df[colname], result[colname]):
            assert datetime.combine(expected, datetime.min.time()) == actual.to_pydatetime()

def generate_blob(length):
    if (random.randint(0, 5) == 0):
        return None
    return random.getrandbits(8*length).to_bytes(length, 'little')

def test_pyarrow_blob(conn_db_readonly : ConnDB) -> None:
    conn, db = conn_db_readonly
    # blobs, blob views, and fixed size blobs
    random.seed(100)
    index = pa.array(range(16000), type=pa.int64())
    col1 = pa.array([generate_blob(random.randint(10, 100)) for i in range(16000)],
        type=pa.binary())
    col2 = pa.array([generate_blob(random.randint(10, 100)) for i in range(16000)],
        type=pa.large_binary())
    col3 = pa.array([generate_blob(32) for i in range(16000)],
        type=pa.binary(32))
    col4 = col1.view(pa.binary())
    df = pd.DataFrame({
        'index': arrowtopd(index),
        'col1' : arrowtopd(col1),
        'col2' : arrowtopd(col2),
        'col3' : arrowtopd(col3),
        'col4' : arrowtopd(col4)
    }).sort_values(by=['index'])
    result = conn.execute('CALL READ_PANDAS(df) RETURN * ORDER BY index').get_as_df()
    for colname in ['col1', 'col2', 'col3', 'col4']:
        for expected, actual in zip(df[colname], result[colname]):
            if is_null(expected) or is_null(actual):
                assert is_null(expected)
                assert is_null(actual)
            else:
                if bytes(expected) != bytes(actual):
                    print(expected)
                    print(actual)
                    print(df[colname])
                    print(result[colname])
                    print(colname)
                assert bytes(expected) == bytes(actual)

def generate_string(length):
    if (random.randint(0, 5) == 0):
        return None
    return ''.join([random.choice('1234567890-=qwertyuiop[]\\asdfghjkl;\'zxcvbnm,./')
        for i in range(length)])

def test_pyarrow_string(conn_db_readonly : ConnDB) -> None:
    conn, db = conn_db_readonly
    # blobs, blob views, and fixed size blobs
    random.seed(100)
    index = pa.array(range(16000), type=pa.int64())
    col1 = pa.array([generate_string(random.randint(10, 100)) for i in range(16000)],
        type=pa.string())
    col2 = pa.array([generate_string(random.randint(10, 100)) for i in range(16000)],
        type=pa.large_string())
    col3 = col1.view(pa.string())
    df = pd.DataFrame({
        'index': arrowtopd(index),
        'col1' : arrowtopd(col1),
        'col2' : arrowtopd(col2),
        'col3' : arrowtopd(col3),
    }).sort_values(by=['index'])
    result = conn.execute('CALL READ_PANDAS(df) RETURN * ORDER BY index').get_as_df()
    for colname in ['col1', 'col2', 'col3']:
        for expected, actual in zip(df[colname], result[colname]):
            if is_null(expected) or is_null(actual):
                assert is_null(expected)
                assert is_null(actual)
            else:
                assert str(expected) == str(actual)

def test_pyarrow_dict(conn_db_readonly : ConnDB) -> None:
    conn, db = conn_db_readonly
    random.seed(100)
    index = pa.array(range(2000), type=pa.int64())
    col1 = pa.array([random.randint(0, 1) for i in range(2000)], type=pa.int32()).dictionary_encode()
    col2 = pa.array([random.randint(-20, 20) / 10 for i in range(2000)], type=pa.float64()).dictionary_encode()
    #it seems arrow hasn't implemented dictionary encoding for nested types
    #col3 = pa.array([
    #    [generate_string(random.randint(10, 100)) for x in range(random.randint(10, 100))]
    #    for i in range(3000)
    #], type=pa.list_(pa.string())).dictionary_encode()
    df = pd.DataFrame({
        'index': arrowtopd(index),
        'col1' : arrowtopd(col1),
        'col2' : arrowtopd(col2)
    })
    result = conn.execute('CALL READ_PANDAS(df) RETURN * ORDER BY index').get_as_df()
    for colname in ['col1', 'col2']:
        for expected, actual in zip(df[colname], result[colname]):
            assert expected == actual

def test_pyarrow_list(conn_db_readonly : ConnDB) -> None:
    conn, db = conn_db_readonly
    random.seed(100)
    datalength = 50
    childlength = 5
    index = pa.array(range(datalength))
    col1 = pa.array(
        [[generate_primitive('int32[pyarrow]') for x in range(random.randint(1, childlength))] if random.randint(0, 5) == 0 else None for i in range(datalength)])
    col2 = pa.array(
        [[[generate_primitive('int32[pyarrow]') for x in range(random.randint(1, childlength))] for y in range(1, childlength)] if random.randint(0, 5) == 0 else None for i in range(datalength)])
    df = pd.DataFrame({
        'index': arrowtopd(index),
        'col1': arrowtopd(col1),
        'col2': arrowtopd(col2)
    })
    result = conn.execute('CALL READ_PANDAS(df) RETURN * ORDER BY index')
    idx = 0
    while result.has_next():
        assert idx < len(index)
        nxt = result.get_next()
        proc = [idx, col1[idx].as_py(), col2[idx].as_py()]
        assert proc == nxt
        idx += 1
    
    assert idx == len(index)

def test_pyarrow_struct(conn_db_readonly : ConnDB) -> None:
    conn, db = conn_db_readonly
    random.seed(100)
    datalength = 4096
    index = pa.array(range(datalength))
    col1_plaindata = [{
            'a': generate_primitive('int32[pyarrow]'),
            'b': { 'c': generate_string(10) } if random.randint(0, 5) != 0 else None
        } if random.randint(0, 5) != 0 else None for i in range(datalength)]
    col1 = pa.array(col1_plaindata, pa.struct([
        ('a', pa.int32()),
        ('b', pa.struct([('c', pa.string())]))
    ]))
    df = pd.DataFrame({
        'index': arrowtopd(index),
        'col1': arrowtopd(col1)
    })
    result = conn.execute('CALL READ_PANDAS(df) RETURN * ORDER BY index')
    idx = 0
    while result.has_next():
        assert idx < len(index)
        nxt = result.get_next()
        expected = [idx, col1[idx].as_py()]
        assert expected == nxt
        idx += 1
    
    assert idx == len(index)

def test_pyarrow_union(conn_db_readonly : ConnDB) -> None:
    pytest.skip("unions are not very well supported by kuzu in general")
    conn, db = conn_db_readonly
    random.seed(100)
    datalength = 4096
    index = pa.array(range(datalength))
    type_codes = pa.array([random.randint(0, 2) for i in range(datalength)], type=pa.int8())
    arr1 = pa.array([generate_primitive('int32[pyarrow]') for i in range(datalength)], type=pa.int32())
    arr2 = pa.array([generate_string(random.randint(1, 10)) for i in range(datalength)])
    arr3 = pa.array([[generate_primitive('float32[pyarrow]') for i in range(10)] for j in range(datalength)])
    col1 = pa.UnionArray.from_sparse(type_codes, [arr1, arr2, arr3])
    df = pd.DataFrame({
        'index': arrowtopd(index),
        'col1': arrowtopd(col1)
    })
    result = conn.execute('CALL READ_PANDAS(df) RETURN * ORDER BY index')
    idx = 0
    while result.has_next():
        assert idx < len(index)
        nxt = result.get_next()
        expected = [idx, col1[idx].as_py()]
        assert expected == nxt
        idx += 1

    assert idx == len(index)

def test_pyarrow_map(conn_db_readonly : ConnDB) -> None:
    conn, db = conn_db_readonly
    random.seed(100)
    datalength = 4096
    index = pa.array(range(datalength))
    keySet = range(100)
    valueSet = 'abcdefghijklmnopqrstuvwxyz'
    col1 = pa.array([
        {str(key) : ''.join(random.sample(valueSet, random.randint(0, len(valueSet))))
            for key in random.sample(keySet, random.randint(1, len(keySet)))}
        if random.randint(0, 5) != 0 else None for i in range(datalength)],
        type=pa.map_(pa.string(), pa.string()))
    df = pd.DataFrame({
        'index': arrowtopd(index),
        'col1': arrowtopd(col1)})
    result = conn.execute('CALL READ_PANDAS(df) RETURN * ORDER BY index')
    idx = 0
    while result.has_next():
        assert idx < len(index)
        nxt = result.get_next()
        expected = [idx, None if col1[idx].as_py() is None else dict(col1[idx].as_py())]
        assert expected == nxt
        idx += 1

    assert idx == len(index)
