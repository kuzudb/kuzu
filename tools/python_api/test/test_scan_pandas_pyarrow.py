import math
import random
import struct
from datetime import datetime, timedelta
from pathlib import Path

import kuzu
import pandas as pd
import pyarrow as pa
import pytest
from pandas.arrays import ArrowExtensionArray as arrowtopd
from type_aliases import ConnDB


def generate_primitive(dtype):
    if random.randrange(0, 5) == 0:
        return None
    if dtype.startswith("bool"):
        return random.randrange(0, 1) == 1
    if dtype.startswith("int32"):
        return random.randrange(-2147483648, 2147483647)
    if dtype.startswith("int64"):
        return random.randrange(-9223372036854775808, 9223372036854775807)
    if dtype.startswith("uint64"):
        return random.randrange(0, 18446744073709551615)
    if dtype.startswith("float32"):
        random_bits = random.getrandbits(32)
        random_bytes = struct.pack("<I", random_bits)
        random_float = struct.unpack("<f", random_bytes)[0]
        return random_float
    return -1


def generate_primitive_series(scale, dtype):
    return pd.Series([generate_primitive(dtype) for i in range(scale)], dtype=dtype)


def generate_primitive_df(scale, names, schema):
    return pd.DataFrame({name: generate_primitive_series(scale, dtype) for name, dtype in zip(names, schema)})


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
        return val == ""
    if type(val) is pd._libs.missing.NAType:
        return True
    if type(val) is float:
        return math.isnan(val)
    return False


def pyarrow_test_helper(establish_connection, n, k):
    conn, db = establish_connection
    names = ["boolcol", "int32col", "int64col", "uint64col", "floatcol"]
    schema = ["bool[pyarrow]", "int32[pyarrow]", "int64[pyarrow]", "uint64[pyarrow]", "float32[pyarrow]"]
    set_thread_count(conn, k)
    random.seed(n * k)
    df = generate_primitive_df(n, names, schema).sort_values(by=["int32col", "int64col", "uint64col", "floatcol"])
    patable = pa.Table.from_pandas(df).select(names)
    result = conn.execute(
        "LOAD FROM df RETURN boolcol, int32col, int64col, uint64col, floatcol ORDER BY int32col, int64col, uint64col, floatcol"
    ).get_as_arrow(n)
    if not tables_equal(patable, result):
        print(patable)
        print("-" * 25)
        print(result)
        print("-" * 25)
        pytest.fail("tables are not equal")


def test_pyarrow_primitive(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    establish_connection = (conn, db)
    # stress tests primitive reading
    sfs = [100, 2048, 4000, 9000, 16000]
    threads = [1, 2, 5, 10]
    for sf in sfs:
        for thread in threads:
            pyarrow_test_helper(establish_connection, sf, thread)


def test_pyarrow_time(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    col1 = pa.array([1000123, 2000123, 3000123], type=pa.duration("s"))
    col2 = pa.array([1000123000000, 2000123000000, 3000123000000], type=pa.duration("us"))
    col3 = pa.array([1000123000000000, 2000123000000000, 3000123000000000], type=pa.duration("ns"))
    col4 = pa.array([datetime(2012, 1, 20), datetime(2000, 12, 2), datetime(1987, 5, 27)], type=pa.timestamp("s"))
    col5 = pa.array([datetime(2012, 1, 20), datetime(2000, 12, 2), datetime(1987, 5, 27)], type=pa.timestamp("s"))
    col6 = pa.array([datetime(2012, 1, 20), datetime(2000, 12, 2), datetime(1987, 5, 27)], type=pa.timestamp("ms"))
    col7 = pa.array([datetime(2012, 1, 20), datetime(2000, 12, 2), datetime(1987, 5, 27)], type=pa.timestamp("us"))
    col8 = pa.array([datetime(2012, 1, 20), datetime(2000, 12, 2), datetime(1987, 5, 27)], type=pa.timestamp("ns"))
    col9 = pa.array([datetime(2012, 1, 20), datetime(2000, 12, 2), datetime(1987, 5, 27)], type=pa.date32())
    col10 = pa.array([datetime(2012, 1, 20), datetime(2000, 12, 2), datetime(1987, 5, 27)], type=pa.date64())
    # not implemented by pandas
    # col11 = pa.array([(1, 2, 3), (4, 5, -6), (100, 200, 1000000000)], type=pa.month_day_nano_interval())
    # for some reason, pyarrow doesnt support the direct creation of pure month or pure datetime
    # intervals, so that will remain untested for now
    df = pd.DataFrame({
        "col1": arrowtopd(col1),
        "col2": arrowtopd(col2),
        "col3": arrowtopd(col3),
        "col4": arrowtopd(col4),
        "col5": arrowtopd(col5),
        "col6": arrowtopd(col6),
        "col7": arrowtopd(col7),
        "col8": arrowtopd(col8),
        "col9": arrowtopd(col9),
        "col10": arrowtopd(col10),
        # 'col11': arrowtopd(col11)
    })
    result = conn.execute("LOAD FROM df RETURN *").get_as_df()
    for colname in ["col1", "col2", "col3"]:
        for expected, actual in zip(df[colname], result[colname]):
            tmp1 = expected if type(expected) is timedelta else expected.to_pytimedelta()
            tmp2 = actual if type(actual) is timedelta else actual.to_pytimedelta()
            assert tmp1 == tmp2
    for colname in ["col4", "col5", "col6", "col7", "col8"]:
        for expected, actual in zip(df[colname], result[colname]):
            tmp1 = expected if type(expected) is datetime else expected.to_pydatetime()
            tmp2 = actual if type(actual) is datetime else actual.to_pydatetime()
            assert tmp1 == tmp2
    for colname in ["col9", "col10"]:
        for expected, actual in zip(df[colname], result[colname]):
            assert datetime.combine(expected, datetime.min.time()) == actual.to_pydatetime()


def generate_blob(length):
    if random.randint(0, 5) == 0:
        return None
    return random.getrandbits(8 * length).to_bytes(length, "little")


def test_pyarrow_blob(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    # blobs, blob views, and fixed size blobs
    random.seed(100)
    index = pa.array(range(16000), type=pa.int64())
    col1 = pa.array([generate_blob(random.randint(10, 100)) for i in range(16000)], type=pa.binary())
    col2 = pa.array([generate_blob(random.randint(10, 100)) for i in range(16000)], type=pa.large_binary())
    col3 = pa.array([generate_blob(32) for i in range(16000)], type=pa.binary(32))
    col4 = col1.view(pa.binary())
    df = pd.DataFrame({
        "index": arrowtopd(index),
        "col1": arrowtopd(col1),
        "col2": arrowtopd(col2),
        "col3": arrowtopd(col3),
        "col4": arrowtopd(col4),
    }).sort_values(by=["index"])
    result = conn.execute("LOAD FROM df RETURN * ORDER BY index").get_as_df()
    for colname in ["col1", "col2", "col3", "col4"]:
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
    if random.randint(0, 5) == 0:
        return None
    return "".join([random.choice("1234567890-=qwertyuiop[]\\asdfghjkl;'zxcvbnm,./") for i in range(length)])


def test_pyarrow_string(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    # blobs, blob views, and fixed size blobs
    random.seed(100)
    index = pa.array(range(16000), type=pa.int64())
    col1 = pa.array([generate_string(random.randint(10, 100)) for i in range(16000)], type=pa.string())
    col2 = pa.array([generate_string(random.randint(10, 100)) for i in range(16000)], type=pa.large_string())
    col3 = col1.view(pa.string())
    df = pd.DataFrame({
        "index": arrowtopd(index),
        "col1": arrowtopd(col1),
        "col2": arrowtopd(col2),
        "col3": arrowtopd(col3),
    }).sort_values(by=["index"])
    result = conn.execute("LOAD FROM df RETURN * ORDER BY index").get_as_df()
    for colname in ["col1", "col2", "col3"]:
        for expected, actual in zip(df[colname], result[colname]):
            if is_null(expected) or is_null(actual):
                assert is_null(expected)
                assert is_null(actual)
            else:
                assert str(expected) == str(actual)


def test_pyarrow_dict(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    random.seed(100)
    index = pa.array(range(2000), type=pa.int64())
    col1 = pa.array([random.randint(0, 1) for i in range(2000)], type=pa.int32()).dictionary_encode()
    col2 = pa.array([random.randint(-20, 20) / 10 for i in range(2000)], type=pa.float64()).dictionary_encode()
    # it seems arrow hasn't implemented dictionary encoding for nested types
    # col3 = pa.array([
    #    [generate_string(random.randint(10, 100)) for x in range(random.randint(10, 100))]
    #    for i in range(3000)
    # ], type=pa.list_(pa.string())).dictionary_encode()
    df = pd.DataFrame({"index": arrowtopd(index), "col1": arrowtopd(col1), "col2": arrowtopd(col2)})
    result = conn.execute("LOAD FROM df RETURN * ORDER BY index").get_as_df()
    for colname in ["col1", "col2"]:
        for expected, actual in zip(df[colname], result[colname]):
            assert expected == actual


def test_pyarrow_dict_offset(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    random.seed(100)
    datalength = 4000
    index = pa.array(range(datalength), type=pa.int64())
    indices = pa.array([random.randint(0, 2) for _ in range(datalength)])
    dictionary = pa.array([1, 2, 3, 4])
    col1 = pa.DictionaryArray.from_arrays(indices, dictionary.slice(1, 3))
    df = pd.DataFrame({"index": arrowtopd(index), "col1": arrowtopd(col1)})
    result = conn.execute("LOAD FROM df RETURN * ORDER BY index")
    idx = 0
    while result.has_next():
        assert idx < len(index)
        nxt = result.get_next()
        proc = [idx, col1[idx].as_py()]
        assert proc == nxt
        idx += 1

    assert idx == len(index)


def test_pyarrow_list(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    random.seed(100)
    datalength = 50
    childlength = 5
    index = pa.array(range(datalength))
    col1 = pa.array([
        [generate_primitive("int32[pyarrow]") for x in range(random.randint(1, childlength))]
        if random.randint(0, 5) == 0
        else None
        for i in range(datalength)
    ])
    col2 = pa.array([
        [
            [generate_primitive("int32[pyarrow]") for x in range(random.randint(1, childlength))]
            for y in range(1, childlength)
        ]
        if random.randint(0, 5) == 0
        else None
        for i in range(datalength)
    ])
    df = pd.DataFrame({"index": arrowtopd(index), "col1": arrowtopd(col1), "col2": arrowtopd(col2)})
    result = conn.execute("LOAD FROM df RETURN * ORDER BY index")
    idx = 0
    while result.has_next():
        assert idx < len(index)
        nxt = result.get_next()
        proc = [idx, col1[idx].as_py(), col2[idx].as_py()]
        assert proc == nxt
        idx += 1

    assert idx == len(index)


def test_pyarrow_list_offset(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    random.seed(100)
    datalength = 50
    childlength = 5
    index = pa.array(range(datalength))
    values = pa.array([generate_primitive("int32[pyarrow]") for _ in range(datalength * childlength + 2)])
    offsets = pa.array(sorted([random.randint(0, datalength * childlength + 1) for _ in range(datalength + 1)]))
    mask = pa.array([random.choice([True, False]) for _ in range(datalength)])
    col1 = pa.ListArray.from_arrays(values=values.slice(2, datalength * childlength), offsets=offsets, mask=mask)
    df = pd.DataFrame({
        "index": arrowtopd(index),
        "col1": arrowtopd(col1),
    })
    result = conn.execute("LOAD FROM df RETURN * ORDER BY index")
    idx = 0
    while result.has_next():
        assert idx < len(index)
        nxt = result.get_next()
        proc = [idx, col1[idx].as_py()]
        assert proc == nxt
        idx += 1

    assert idx == len(index)


def test_pyarrow_fixed_list(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    random.seed(100)
    data_len = 50
    child_len = 3

    mask = pa.array([random.choice([True, False]) for _ in range(data_len)])
    index = pa.array(range(data_len))

    # fixed list of primitive
    primitive_values = pa.array([generate_primitive("int32[pyarrow]") for _ in range(data_len * child_len)])
    primitive_col = pa.FixedSizeListArray.from_arrays(primitive_values, list_size=child_len, mask=mask)

    # fixed list of datetime
    datetime_values = pa.array(
        [
            datetime(random.randint(1900, 2023), random.randint(1, 12), random.randint(1, 28))
            for _ in range(data_len * child_len)
        ],
        type=pa.date32(),
    )
    datetime_col = pa.FixedSizeListArray.from_arrays(datetime_values, list_size=child_len, mask=mask)

    # fixed list of blob
    blob_values = pa.array(
        [generate_blob(random.randint(10, 100)) for _ in range(data_len * child_len)], type=pa.binary()
    )
    blob_col = pa.FixedSizeListArray.from_arrays(blob_values, list_size=child_len, mask=mask)

    # fixed list of string
    string_values = pa.array(
        [generate_string(random.randint(10, 100)) for _ in range(data_len * child_len)], type=pa.string()
    )
    string_col = pa.FixedSizeListArray.from_arrays(string_values, list_size=child_len, mask=mask)

    # fixed list of dict
    dict_values = pa.array(
        [random.randint(0, 1) for _ in range(data_len * child_len)], type=pa.int32()
    ).dictionary_encode()
    dict_col = pa.FixedSizeListArray.from_arrays(dict_values, list_size=child_len, mask=mask)

    # fixed list of list
    list_values = pa.array([
        [generate_primitive("int32[pyarrow]") for _ in range(random.randint(1, 5))]
        if random.randint(0, 5) != 0
        else None
        for x in range(data_len * child_len)
    ])
    list_col = pa.FixedSizeListArray.from_arrays(list_values, list_size=child_len, mask=mask)

    # fixed list of fixed list
    fixed_list_col = pa.FixedSizeListArray.from_arrays(primitive_col, list_size=1, mask=mask)

    # fixed lisr of struct
    struct_plaindata = [
        {
            "a": generate_primitive("int32[pyarrow]"),
            "b": {"c": generate_string(10)} if random.randint(0, 5) != 0 else None,
        }
        if random.randint(0, 5) != 0
        else None
        for _ in range(data_len * child_len)
    ]
    struct_values = pa.array(struct_plaindata, pa.struct([("a", pa.int32()), ("b", pa.struct([("c", pa.string())]))]))
    struct_col = pa.FixedSizeListArray.from_arrays(struct_values, list_size=child_len, mask=mask)

    # fixed list of map
    keySet = range(10)
    valueSet = "abcdefghijklmnopqrstuvwxyz"
    map_values = pa.array(
        [
            {
                str(key): "".join(random.sample(valueSet, random.randint(0, len(valueSet))))
                for key in random.sample(keySet, random.randint(1, len(keySet)))
            }
            if random.randint(0, 5) != 0
            else None
            for i in range(data_len * child_len)
        ],
        type=pa.map_(pa.string(), pa.string()),
    )
    map_col = pa.FixedSizeListArray.from_arrays(map_values, list_size=child_len, mask=mask)

    df = pd.DataFrame({
        "index": arrowtopd(index),
        "primitive_col": arrowtopd(primitive_col),
        "datetime_col": arrowtopd(datetime_col),
        "blob_col": arrowtopd(blob_col),
        "string_col": arrowtopd(string_col),
        "dict_col": arrowtopd(dict_col),
        "list_col": arrowtopd(list_col),
        "fixed_list_col": arrowtopd(fixed_list_col),
        "struct_col": arrowtopd(struct_col),
        "map_col": arrowtopd(map_col),
    })
    result = conn.execute("LOAD FROM df RETURN * ORDER BY index")

    idx = 0
    while result.has_next():
        assert idx < len(index)
        nxt = result.get_next()
        proc = [
            idx,
            primitive_col[idx].as_py(),
            datetime_col[idx].as_py(),
            blob_col[idx].as_py(),
            string_col[idx].as_py(),
            dict_col[idx].as_py(),
            list_col[idx].as_py(),
            fixed_list_col[idx].as_py(),
            struct_col[idx].as_py(),
            None
            if map_col[idx].as_py() is None
            else [
                None if map_col[idx][i].as_py() is None else dict(map_col[idx][i].as_py()) for i in range(child_len)
            ],
        ]
        assert proc == nxt
        idx += 1

    assert idx == len(index)


def test_pyarrow_fixed_list_offset(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    random.seed(100)
    data_len = 50
    child_len = 5
    values = pa.array([generate_primitive("int32[pyarrow]") for _ in range(data_len * child_len + 2)])
    mask = pa.array([random.choice([True, False]) for _ in range(data_len)])
    index = pa.array(range(data_len))
    _col1 = pa.FixedSizeListArray.from_arrays(values.slice(2, data_len * child_len), list_size=child_len)
    col1 = _col1.slice(1, 49)
    _col2 = pa.FixedSizeListArray.from_arrays(values.slice(1, data_len * child_len), list_size=child_len, mask=mask)
    col2 = _col2.slice(1, 49)
    df = pd.DataFrame({"index": arrowtopd(index.slice(0, 49)), "col1": arrowtopd(col1), "col2": arrowtopd(col2)})
    result = conn.execute("LOAD FROM df RETURN * ORDER BY index")
    idx = 0
    while result.has_next():
        assert idx < len(index)
        nxt = result.get_next()
        proc = [idx, col1[idx].as_py(), col2[idx].as_py()]
        assert proc == nxt
        idx += 1

    assert idx == 49


def test_pyarrow_struct(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    random.seed(100)
    datalength = 4096
    index = pa.array(range(datalength))
    col1_plaindata = [
        {
            "a": generate_primitive("int32[pyarrow]"),
            "b": {"c": generate_string(10)} if random.randint(0, 5) != 0 else None,
        }
        if random.randint(0, 5) != 0
        else None
        for i in range(datalength)
    ]
    col1 = pa.array(col1_plaindata, pa.struct([("a", pa.int32()), ("b", pa.struct([("c", pa.string())]))]))
    df = pd.DataFrame({"index": arrowtopd(index), "col1": arrowtopd(col1)})
    result = conn.execute("LOAD FROM df RETURN * ORDER BY index")
    idx = 0
    while result.has_next():
        assert idx < len(index)
        nxt = result.get_next()
        expected = [idx, col1[idx].as_py()]
        assert expected == nxt
        idx += 1

    assert idx == len(index)


def test_pyarrow_struct_offset(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    random.seed(100)
    datalength = 4096
    index = pa.array(range(datalength))
    val1 = pa.array([generate_primitive("int32[pyarrow]") for _ in range(datalength + 1)])
    val2 = pa.array([generate_primitive("bool[pyarrow]") for _ in range(datalength + 2)])
    val3 = pa.array([generate_string(random.randint(5, 10)) for _ in range(datalength + 3)])
    mask = pa.array([random.choice([True, False]) for _ in range(datalength)])
    col1 = pa.StructArray.from_arrays(
        [val1.slice(1, datalength), val2.slice(2, datalength), val3.slice(3, datalength)],
        names=["a", "b", "c"],
        mask=mask,
    )
    df = pd.DataFrame({"index": arrowtopd(index), "col1": arrowtopd(col1)})
    result = conn.execute("LOAD FROM df RETURN * ORDER BY index")
    idx = 0
    while result.has_next():
        assert idx < len(index)
        nxt = result.get_next()
        expected = [idx, col1[idx].as_py()]
        assert expected == nxt
        idx += 1

    assert idx == len(index)


def test_pyarrow_union_sparse(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    random.seed(100)
    datalength = 4096
    index = pa.array(range(datalength))
    type_codes = pa.array([random.randint(0, 2) for i in range(datalength)], type=pa.int8())
    arr1 = pa.array([generate_primitive("int32[pyarrow]") for i in range(datalength + 1)], type=pa.int32())
    arr2 = pa.array([generate_string(random.randint(1, 10)) for i in range(datalength + 2)])
    arr3 = pa.array([generate_primitive("float32[pyarrow]") for j in range(datalength + 3)])
    col1 = pa.UnionArray.from_sparse(
        type_codes, [arr1.slice(1, datalength), arr2.slice(2, datalength), arr3.slice(3, datalength)]
    )
    df = pd.DataFrame({"index": arrowtopd(index), "col1": arrowtopd(col1)})
    result = conn.execute("LOAD FROM df RETURN * ORDER BY index")
    idx = 0
    while result.has_next():
        assert idx < len(index)
        nxt = result.get_next()
        expected = [idx, col1[idx].as_py()]
        assert expected == nxt or (is_null(nxt[1]) and is_null(expected[1]))
        idx += 1

    assert idx == len(index)


def test_pyarrow_union_dense(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    random.seed(100)
    datalength = 4096
    index = pa.array(range(datalength))
    _type_codes = [random.randint(0, 2) for i in range(datalength)]
    type_codes = pa.array(_type_codes, type=pa.int8())
    _offsets = [0 for _ in range(datalength)]
    _cnt = [0, 0, 0]
    for i in range(len(_type_codes)):
        _offsets[i] = _cnt[_type_codes[i]]
        _cnt[_type_codes[i]] += 1
    offsets = pa.array(_offsets, type=pa.int32())
    arr1 = pa.array([generate_primitive("int32[pyarrow]") for i in range(datalength + 1)], type=pa.int32())
    arr2 = pa.array([generate_string(random.randint(1, 10)) for i in range(datalength + 2)])
    arr3 = pa.array([generate_primitive("float32[pyarrow]") for j in range(datalength + 3)])
    col1 = pa.UnionArray.from_dense(
        type_codes, offsets, [arr1.slice(1, datalength), arr2.slice(2, datalength), arr3.slice(3, datalength)]
    )
    df = pd.DataFrame({"index": arrowtopd(index), "col1": arrowtopd(col1)})
    result = conn.execute("LOAD FROM df RETURN * ORDER BY index")
    idx = 0
    while result.has_next():
        assert idx < len(index)
        nxt = result.get_next()
        expected = [idx, col1[idx].as_py()]
        assert expected == nxt or (is_null(nxt[1]) and is_null(expected[1]))
        idx += 1

    assert idx == len(index)


def test_pyarrow_map(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    random.seed(100)
    datalength = 4096
    index = pa.array(range(datalength))
    keySet = range(100)
    valueSet = "abcdefghijklmnopqrstuvwxyz"
    col1 = pa.array(
        [
            {
                str(key): "".join(random.sample(valueSet, random.randint(0, len(valueSet))))
                for key in random.sample(keySet, random.randint(1, len(keySet)))
            }
            if random.randint(0, 5) != 0
            else None
            for i in range(datalength)
        ],
        type=pa.map_(pa.string(), pa.string()),
    )
    df = pd.DataFrame({"index": arrowtopd(index), "col1": arrowtopd(col1)})
    result = conn.execute("LOAD FROM df RETURN * ORDER BY index")
    idx = 0
    while result.has_next():
        assert idx < len(index)
        nxt = result.get_next()
        expected = [idx, None if col1[idx].as_py() is None else dict(col1[idx].as_py())]
        assert expected == nxt
        idx += 1

    assert idx == len(index)


def test_pyarrow_map_offset(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    random.seed(100)
    datalength = 50
    maplength = 5
    index = pa.array(range(datalength))
    offsets = sorted([random.randint(0, datalength * maplength + 1) for _ in range(datalength + 1)])
    offsets[25] = None
    offsets = pa.array(offsets, type=pa.int32())
    keys = pa.array([random.randint(0, (1 << 31) - 1) for _ in range(datalength * maplength + 1)])
    values = pa.array([generate_primitive("int64[pyarrow]") for _ in range(datalength * maplength + 1)])
    _col1 = pa.MapArray.from_arrays(
        offsets, keys.slice(1, datalength * maplength), values.slice(1, datalength * maplength)
    )
    col1 = _col1.slice(2, 48)
    df = pd.DataFrame({
        "index": arrowtopd(index.slice(0, 48)),
        "col1": arrowtopd(col1),
    })
    result = conn.execute("LOAD FROM df RETURN * ORDER BY index")
    idx = 0
    while result.has_next():
        assert idx < len(index)
        nxt = result.get_next()
        expected = [idx, None if col1[idx].as_py() is None else dict(col1[idx].as_py())]
        assert expected == nxt
        idx += 1

    assert idx == 48
