import datetime
import re
from pathlib import Path
from uuid import UUID

import numpy as np
import pandas as pd
import pytest

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore[no-redef]

import kuzu


def validate_scan_pandas_results(results: kuzu.QueryResult) -> None:
    assert results.get_next() == [
        True,
        1,
        10,
        100,
        1000,
        -1,
        -10,
        -100,
        -1000,
        -0.5199999809265137,
        5132.12321,
        datetime.datetime(1996, 4, 1, 12, 0, 11, 500001),
        datetime.datetime(1996, 4, 1, 12, 0, 11, 500001, ZoneInfo("US/Eastern")),
        datetime.datetime(1996, 4, 1, 12, 0, 11, 500001),
        datetime.datetime(1996, 4, 1, 12, 0, 11, 500000),
        datetime.datetime(1996, 4, 1, 12, 0, 11),
        datetime.timedelta(microseconds=500),
        None,
        [],
        528,
        3.562,
        ["Alice", None],
        datetime.date(1996, 2, 15),
        "12331",
        UUID("d5a8ed71-6fc4-4cb3-acbc-2f5b73fd14bc"),
    ]
    assert results.get_next() == [
        False,
        2,
        20,
        200,
        2000,
        -2,
        -20,
        -200,
        -2000,
        None,
        24.222,
        datetime.datetime(1981, 11, 13, 22, 2, 52, 2),
        datetime.datetime(1981, 11, 13, 22, 2, 52, 2, ZoneInfo("US/Eastern")),
        datetime.datetime(1981, 11, 13, 22, 2, 52, 2),
        datetime.datetime(1981, 11, 13, 22, 2, 52),
        datetime.datetime(1981, 11, 13, 22, 2, 52),
        datetime.timedelta(seconds=1),
        "Ascii only",
        [40, 20, 10],
        -9999,
        4.213,
        [],
        datetime.date(2013, 2, 22),
        "test string",
        UUID("9a2fc988-5c5d-4217-af9e-220aef5ce7b8"),
    ]
    assert results.get_next() == [
        None,
        3,
        30,
        300,
        3000,
        -3,
        -30,
        -300,
        -3000,
        -3.299999952316284,
        None,
        datetime.datetime(1972, 12, 21, 12, 5, 44, 500003),
        datetime.datetime(1972, 12, 21, 12, 5, 44, 500003, ZoneInfo("US/Eastern")),
        datetime.datetime(1972, 12, 21, 12, 5, 44, 500003),
        datetime.datetime(1972, 12, 21, 12, 5, 44, 500000),
        datetime.datetime(1972, 12, 21, 12, 5, 44),
        datetime.timedelta(seconds=2, milliseconds=500),
        "ñ中国字",
        [30, None],
        None,
        None,
        None,
        datetime.date(2055, 1, 14),
        "5.623",
        UUID("166055ee-a481-4e67-a4fc-98682d3a3e20"),
    ]
    assert results.get_next() == [
        False,
        4,
        40,
        400,
        4000,
        -4,
        -40,
        -400,
        -4000,
        4.400000095367432,
        4.444,
        datetime.datetime(2008, 1, 11, 22, 10, 3, 4),
        datetime.datetime(2008, 1, 11, 22, 10, 3, 4, ZoneInfo("US/Eastern")),
        datetime.datetime(2008, 1, 11, 22, 10, 3, 4),
        datetime.datetime(2008, 1, 11, 22, 10, 3),
        datetime.datetime(2008, 1, 11, 22, 10, 3),
        datetime.timedelta(seconds=3, milliseconds=22),
        "😂",
        None,
        56677,
        67.13,
        ["Dan, Ella", "George"],
        datetime.date(2018, 3, 17),
        None,
        UUID("d5a8ed71-6fc4-4cb3-acbc-2f5b73fd14bc"),
    ]


def test_scan_pandas(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    data = {
        "BOOL": [True, False, None, False],
        "UINT8": np.array([1, 2, 3, 4], dtype=np.uint8),
        "UINT16": np.array([10, 20, 30, 40], dtype=np.uint16),
        "UINT32": np.array([100, 200, 300, 400], dtype=np.uint32),
        "UINT64": np.array([1000, 2000, 3000, 4000], dtype=np.uint64),
        "INT8": np.array([-1, -2, -3, -4], dtype=np.int8),
        "INT16": np.array([-10, -20, -30, -40], dtype=np.int16),
        "INT32": np.array([-100, -200, -300, -400], dtype=np.int32),
        "INT64": np.array([-1000, -2000, -3000, -4000], dtype=np.int64),
        "FLOAT_32": np.array(
            [-0.5199999809265137, float("nan"), -3.299999952316284, 4.400000095367432],
            dtype=np.float32,
        ),
        "FLOAT_64": np.array([5132.12321, 24.222, float("nan"), 4.444], dtype=np.float64),
        "datetime_microseconds": np.array([
            np.datetime64("1996-04-01T12:00:11.500001000"),
            np.datetime64("1981-11-13T22:02:52.000002000"),
            np.datetime64("1972-12-21T12:05:44.500003000"),
            np.datetime64("2008-01-11T22:10:03.000004000"),
        ]).astype("datetime64[us]"),
        "datetime_microseconds_tz": np.array([
            np.datetime64("1996-04-01T12:00:11.500001000"),
            np.datetime64("1981-11-13T22:02:52.000002000"),
            np.datetime64("1972-12-21T12:05:44.500003000"),
            np.datetime64("2008-01-11T22:10:03.000004000"),
        ]).astype("datetime64[us]"),
        "datetime_nanoseconds": np.array([
            np.datetime64("1996-04-01T12:00:11.500001"),
            np.datetime64("1981-11-13T22:02:52.000002"),
            np.datetime64("1972-12-21T12:05:44.500003"),
            np.datetime64("2008-01-11T22:10:03.000004"),
        ]).astype("datetime64[ns]"),
        "datetime_milliseconds": np.array([
            np.datetime64("1996-04-01T12:00:11.500001"),
            np.datetime64("1981-11-13T22:02:52.000002"),
            np.datetime64("1972-12-21T12:05:44.500003"),
            np.datetime64("2008-01-11T22:10:03.000004"),
        ]).astype("datetime64[ms]"),
        "datetime_seconds": np.array([
            np.datetime64("1996-04-01T12:00:11"),
            np.datetime64("1981-11-13T22:02:52"),
            np.datetime64("1972-12-21T12:05:44"),
            np.datetime64("2008-01-11T22:10:03"),
        ]).astype("datetime64[s]"),
        "timedelta_nanoseconds": [
            np.timedelta64(500000, "ns"),
            np.timedelta64(1000000000, "ns"),
            np.timedelta64(2500000000, "ns"),
            np.timedelta64(3022000000, "ns"),
        ],
        "name": [None, "Ascii only", "ñ中国字", "😂"],
        "worked_hours": [[], [40, 20, 10], [30, None], None],
        "int_object": np.array([528, -9999, None, 56677], dtype=object),
        "float_object": np.array([3.562, 4.213, None, 67.13], dtype=object),
        "used_names": np.array([["Alice", None], [], None, ["Dan, Ella", "George"]], dtype=object),
        "past_date": np.array(
            [
                datetime.date(1996, 2, 15),
                datetime.date(2013, 2, 22),
                datetime.date(2055, 1, 14),
                datetime.date(2018, 3, 17),
            ],
            dtype=object,
        ),
        "mixed_type": np.array([12331, "test string", 5.623, None], dtype="object"),
        "uuid_type": [
            UUID("d5a8ed71-6fc4-4cb3-acbc-2f5b73fd14bc"),
            UUID("9a2fc988-5c5d-4217-af9e-220aef5ce7b8"),
            UUID("166055ee-a481-4e67-a4fc-98682d3a3e20"),
            UUID("d5a8ed71-6fc4-4cb3-acbc-2f5b73fd14bc"),
        ],
    }
    df = pd.DataFrame(data)
    df["datetime_microseconds_tz"] = df["datetime_microseconds_tz"].dt.tz_localize("US/Eastern")
    results = conn.execute("LOAD FROM df RETURN *")
    validate_scan_pandas_results(results)

    results_parameterized = conn.execute("LOAD FROM $df RETURN *", {"df": df})
    validate_scan_pandas_results(results_parameterized)


def test_scan_pandas_timestamp(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    ts = np.array(
        [
            datetime.datetime(1996, 2, 15, hour=12, minute=22, second=54),
            datetime.datetime(2011, 3, 11, minute=11, hour=5),
            None,
            datetime.datetime(2033, 2, 11, microsecond=55),
        ],
        dtype="object",
    )
    df = pd.DataFrame({"timestamp": ts})
    # Pandas automatically converts the column from object to timestamp, so we need to manually cast back to object.
    df = df.astype({"timestamp": "object"}, copy=False)
    results = conn.execute("LOAD FROM df RETURN *")
    assert results.get_next() == [datetime.datetime(1996, 2, 15, hour=12, minute=22, second=54)]
    assert results.get_next() == [datetime.datetime(2011, 3, 11, minute=11, hour=5)]
    assert results.get_next() == [None]
    assert results.get_next() == [datetime.datetime(2033, 2, 11, microsecond=55)]


def test_replace_failure(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)

    with pytest.raises(RuntimeError, match=re.escape("Binder exception: Variable x is not in scope.")):
        conn.execute("LOAD FROM x RETURN *;")

    with pytest.raises(
        RuntimeError,
        match=re.escape(
            "Binder exception: Cannot match a built-in function for given function "
            "READ_PANDAS(STRING). Supported inputs are\n(POINTER)\n"
        ),
    ):
        conn.execute("CALL READ_PANDAS('df213') WHERE id > 20 RETURN id + 5, weight")


def test_int64_overflow(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    overflowpd = pd.DataFrame({"id": [4, 2**125]})
    with pytest.raises(
        RuntimeError,
        match=re.escape(
            "Conversion exception: Failed to cast value: "
            "Python value '42535295865117307932921825928971026432' to INT64"
        ),
    ):
        conn.execute("LOAD FROM overflowpd RETURN *;")


def test_scan_pandas_with_filter(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    data = {
        "id": np.array([22, 3, 100], dtype=np.uint8),
        "weight": np.array([23.2, 31.7, 42.9], dtype=np.float64),
        "name": ["ñ", "日本字", "😊"],
    }
    df = pd.DataFrame(data)
    # Dummy query to ensure the READ_PANDAS function is persistent after a write transaction.
    conn.execute("CREATE NODE TABLE PERSON1(ID INT64, PRIMARY KEY(ID))")
    results = conn.execute("LOAD FROM df WHERE id > 20 RETURN id + 5, weight, name")
    assert results.get_next() == [27, 23.2, "ñ"]
    assert results.get_next() == [105, 42.9, "😊"]


def test_large_pd(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    num_rows = 40000
    odd_numbers = [2 * i + 1 for i in range(num_rows)]
    even_numbers = [2 * i for i in range(num_rows)]
    df = pd.DataFrame({
        "odd": np.array(odd_numbers, dtype=np.int64),
        "even": np.array(even_numbers, dtype=np.int64),
    })
    result = conn.execute("LOAD FROM df RETURN *").get_as_df()
    assert result["odd"].to_list() == odd_numbers
    assert result["even"].to_list() == even_numbers


def test_pandas_scan_demo(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)

    conn.execute("CREATE NODE TABLE student (ID int64, height int32, PRIMARY KEY(ID))")
    conn.execute("CREATE (s:student {ID: 0, height: 70})")
    conn.execute("CREATE (s:student {ID: 2, height: 64})")
    conn.execute("CREATE (s:student {ID: 4, height: 67})")
    conn.execute("CREATE (s:student {ID: 5, height: 64})")

    id = np.array([0, 2, 3, 5, 7, 11, 13], dtype=np.int64)
    age = np.array([42, 23, 33, 57, 67, 39, 11], dtype=np.uint16)
    height_in_cm = np.array([167, 172, 183, 199, 149, 154, 165], dtype=np.uint32)
    is_student = np.array([False, True, False, False, False, False, True], dtype=bool)
    person = pd.DataFrame({"id": id, "age": age, "height": height_in_cm, "is_student": is_student})

    result = conn.execute(
        "LOAD FROM person with avg(height / 2.54) as height_in_inch MATCH (s:student) WHERE s.height > "
        "height_in_inch RETURN s"
    ).get_as_df()
    assert len(result) == 2
    assert result["s"][0] == {
        "ID": 0,
        "_id": {"offset": 0, "table": 0},
        "_label": "student",
        "height": 70,
    }
    assert result["s"][1] == {
        "ID": 4,
        "_id": {"offset": 2, "table": 0},
        "_label": "student",
        "height": 67,
    }

    conn.execute("CREATE NODE TABLE person(ID INT64, age UINT16, height UINT32, is_student BOOLean, PRIMARY KEY(ID))")
    conn.execute("LOAD FROM person CREATE (p:person {ID: id, age: age, height: height, is_student: is_student})")
    result = conn.execute("MATCH (p:person) return p.*").get_as_df()
    assert np.all(result["p.ID"].to_list() == id)
    assert np.all(result["p.age"].to_list() == age)
    assert np.all(result["p.height"].to_list() == height_in_cm)
    assert np.all(result["p.is_student"].to_list() == is_student)


def test_scan_pandas_copy_subquery(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    data = {"id": np.array([22, 3, 100], dtype=np.int64), "name": ["A", "B", "C"]}
    df = pd.DataFrame(data)
    conn.execute("CREATE NODE TABLE person(ID INT64, NAME STRING, PRIMARY KEY(ID))")
    conn.execute("COPY person FROM (LOAD FROM df RETURN *)")
    result = conn.execute("MATCH (p:person) RETURN p.*").get_as_df()
    assert result["p.ID"].to_list() == [22, 3, 100]
    assert result["p.NAME"].to_list() == ["A", "B", "C"]


def test_scan_all_null(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    data = {"id": np.array([None, None, None], dtype=object)}
    df = pd.DataFrame(data)
    result = conn.execute("LOAD FROM df RETURN *")
    assert result.get_next() == [None]
    assert result.get_next() == [None]
    assert result.get_next() == [None]


def test_copy_from_scan_pandas_result(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    df = pd.DataFrame({"name": ["Adam", "Karissa", "Zhang", "Noura"], "age": [30, 40, 50, 25]})
    conn.execute("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name));")
    conn.execute("COPY Person FROM (LOAD FROM df WHERE age < 30 RETURN *);")
    result = conn.execute("match (p:Person) return p.*")
    assert result.get_next() == ["Noura", 25]
    assert result.has_next() is False


def test_scan_from_py_arrow_pandas(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    df = pd.DataFrame({"name": ["Adam", "Karissa", "Zhang", "Noura"], "age": [30, 40, 50, 25]}).convert_dtypes(
        dtype_backend="pyarrow"
    )
    result = conn.execute("LOAD FROM df RETURN *;")
    assert result.get_next() == ["Adam", 30]
    assert result.get_next() == ["Karissa", 40]
    assert result.get_next() == ["Zhang", 50]
    assert result.get_next() == ["Noura", 25]
    assert result.has_next() is False


def test_scan_long_utf8_string(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    data = {"name": ["很长的一段中文", "短", "非常长的中文"]}
    df = pd.DataFrame(data)
    result = conn.execute("LOAD FROM df WHERE name = '非常长的中文' RETURN count(*);")
    assert result.get_next() == [1]


def test_copy_from_pandas_object(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    df = pd.DataFrame({"name": ["Adam", "Karissa", "Zhang", "Noura"], "age": [30, 40, 50, 25]})
    conn.execute("CREATE NODE TABLE Person(name STRING, age STRING, PRIMARY KEY (name));")
    conn.execute("COPY Person FROM df;")
    result = conn.execute("match (p:Person) return p.*")
    assert result.get_next() == ["Adam", "30"]
    assert result.get_next() == ["Karissa", "40"]
    assert result.get_next() == ["Zhang", "50"]
    assert result.get_next() == ["Noura", "25"]
    assert result.has_next() is False
    df = pd.DataFrame({"f": ["Adam", "Karissa"], "t": ["Zhang", "Zhang"]})
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person);")
    conn.execute("COPY Knows FROM df")
    result = conn.execute("match (p:Person)-[]->(:Person {name: 'Zhang'}) return p.*")
    assert result.get_next() == ["Adam", "30"]
    assert result.get_next() == ["Karissa", "40"]
    assert result.has_next() is False


def test_copy_from_pandas_object_skip(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    df = pd.DataFrame({"name": ["Adam", "Karissa", "Zhang", "Noura"], "age": [30, 40, 50, 25]})
    conn.execute("CREATE NODE TABLE Person(name STRING, age STRING, PRIMARY KEY (name));")
    conn.execute("COPY Person FROM df(SKIP=2);")
    result = conn.execute("match (p:Person) return p.*")
    assert result.get_next() == ["Zhang", "50"]
    assert result.get_next() == ["Noura", "25"]
    assert result.has_next() is False
    df = pd.DataFrame({"f": ["Adam", "Noura"], "t": ["Zhang", "Zhang"]})
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person);")
    conn.execute("COPY Knows FROM df(SKIP=1)")
    result = conn.execute("match (p:Person)-[]->(:Person {name: 'Zhang'}) return p.*")
    assert result.get_next() == ["Noura", "25"]
    assert result.has_next() is False


def test_copy_from_pandas_object_limit(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    df = pd.DataFrame({"name": ["Adam", "Karissa", "Zhang", "Noura"], "age": [30, 40, 50, 25]})
    conn.execute("CREATE NODE TABLE Person(name STRING, age STRING, PRIMARY KEY (name));")
    conn.execute("COPY Person FROM df(LIMIT=2);")
    result = conn.execute("match (p:Person) return p.*")
    assert result.get_next() == ["Adam", "30"]
    assert result.get_next() == ["Karissa", "40"]
    assert result.has_next() is False
    df = pd.DataFrame({"f": ["Adam", "Zhang"], "t": ["Karissa", "Karissa"]})
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person);")
    conn.execute("COPY Knows FROM df(LIMIT=1)")
    result = conn.execute("match (p:Person)-[]->(:Person {name: 'Karissa'}) return p.*")
    assert result.get_next() == ["Adam", "30"]
    assert result.has_next() is False


def test_copy_from_pandas_object_skip_and_limit(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    df = pd.DataFrame({"name": ["Adam", "Karissa", "Zhang", "Noura"], "age": [30, 40, 50, 25]})
    conn.execute("CREATE NODE TABLE Person(name STRING, age STRING, PRIMARY KEY (name));")
    conn.execute("COPY Person FROM df(SKIP=1, LIMIT=2);")
    result = conn.execute("match (p:Person) return p.*")
    assert result.get_next() == ["Karissa", "40"]
    assert result.get_next() == ["Zhang", "50"]
    assert result.has_next() is False


def test_copy_from_pandas_object_skip_bounds_check(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    df = pd.DataFrame({"name": ["Adam", "Karissa", "Zhang", "Noura"], "age": [30, 40, 50, 25]})
    conn.execute("CREATE NODE TABLE Person(name STRING, age STRING, PRIMARY KEY (name));")
    conn.execute("COPY Person FROM df(SKIP=10);")
    result = conn.execute("match (p:Person) return p.*")
    assert result.has_next() is False


def test_copy_from_pandas_object_limit_bounds_check(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    df = pd.DataFrame({"name": ["Adam", "Karissa", "Zhang", "Noura"], "age": [30, 40, 50, 25]})
    conn.execute("CREATE NODE TABLE Person(name STRING, age STRING, PRIMARY KEY (name));")
    conn.execute("COPY Person FROM df(LIMIT=10);")
    result = conn.execute("match (p:Person) return p.*")
    assert result.get_next() == ["Adam", "30"]
    assert result.get_next() == ["Karissa", "40"]
    assert result.get_next() == ["Zhang", "50"]
    assert result.get_next() == ["Noura", "25"]
    assert result.has_next() is False


def test_copy_from_pandas_date(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    df = pd.DataFrame({"id": [1, 2], "date": [pd.Timestamp("2024-01-03"), pd.Timestamp("2023-10-10")]})
    conn.execute("CREATE NODE TABLE Person(id INT16, d TIMESTAMP, PRIMARY KEY (id));")
    conn.execute("COPY Person FROM df;")
    result = conn.execute("match (p:Person) return p.*")
    assert result.get_next() == [1, datetime.datetime(2024, 1, 3)]
    assert result.get_next() == [2, datetime.datetime(2023, 10, 10)]
    assert result.has_next() is False


def test_scan_string_to_nested(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    df = pd.DataFrame({
        "id": ["1"],
        "lstcol": ["[1,2,3]"],
        "mapcol": ["{'a'=1,'b'=2}"],
        "structcol": ["{a:1,b:2}"],
        "lstlstcol": ["[[],[1,2,3],[4,5,6]]"],
    })
    conn.execute(
        "CREATE NODE TABLE tab(id INT64, lstcol INT64[], mapcol MAP(STRING, INT64), structcol STRUCT(a INT64, b INT64), lstlstcol INT64[][], PRIMARY KEY(id))"
    )
    conn.execute("COPY tab from df")
    result = conn.execute("match (t:tab) return t.*")
    assert result.get_next() == [
        1,
        [1, 2, 3],
        {"'a'": 1, "'b'": 2},
        {"a": 1, "b": 2},
        [[], [1, 2, 3], [4, 5, 6]],
    ]
    assert not result.has_next()


def test_pandas_scan_ignore_errors(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    df = pd.DataFrame({"id": [1, 2, 3, 1]})
    conn.execute("CREATE NODE TABLE person(id INT64, PRIMARY KEY(id))")
    conn.execute("COPY person FROM df(IGNORE_ERRORS=true)")

    people = conn.execute("MATCH (p:person) RETURN p.id")
    assert people.get_next() == [1]
    assert people.get_next() == [2]
    assert people.get_next() == [3]
    assert not people.has_next()

    warnings = conn.execute("CALL show_warnings() RETURN *")
    assert warnings.get_next()[1].startswith("Found duplicated primary key value 1")
    assert not warnings.has_next()


def test_copy_from_pandas_multi_pairs(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    conn.execute("CREATE NODE TABLE person(id INT64, PRIMARY KEY(id))")
    conn.execute("CREATE (p:person {id: 3});")
    conn.execute("CREATE (p:person {id: 4});")
    conn.execute("CREATE NODE TABLE student(id INT64, PRIMARY KEY(id))")
    conn.execute("CREATE (p:student {id: 2});")
    conn.execute("CREATE REL TABLE knows(from person to person, from person to student, length int64)")
    df = pd.DataFrame({"from": [3], "to": [4], "length": [252]})
    conn.execute("COPY knows from df (from = 'person', to = 'person');")
    result = conn.execute("match (:person)-[e:knows]->(:person) return e.*")
    assert result.has_next()
    assert result.get_next()[0] == 252
    assert not result.has_next()


def test_scan_pandas_with_exists(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    conn.execute("CREATE NODE TABLE person(id INT64, PRIMARY KEY(id))")
    conn.execute("CREATE (p:person {id: 1})")
    conn.execute("CREATE (p:person {id: 2})")
    conn.execute("CREATE (p:person {id: 3})")
    conn.execute("CREATE REL TABLE knows(from person to person)")
    df = pd.DataFrame({
        "from": [1, 2, 3],
        "to": [3, 2, 1],
    })
    conn.execute(
        "COPY knows from (load from df where not exists {MATCH (p:person)-[:knows]->(p1:person) WHERE p.id = from AND p1.id = to} return from + 1 - 1, to)"
    )
    res = conn.execute("MATCH (p:person)-[:knows]->(p1:person) return p.id, p1.id order by p.id, p1.id")
    assert res.has_next()
    tp = res.get_next()
    assert tp[0] == 1
    assert tp[1] == 3
    tp = res.get_next()
    assert tp[0] == 2
    assert tp[1] == 2
    tp = res.get_next()
    assert tp[0] == 3
    assert tp[1] == 1


def test_scan_empty_list(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    df = pd.DataFrame({"id": ["1"], "lstcol": [[]]})
    res = conn.execute("load from df return *")
    assert res.has_next()
    tp = res.get_next()
    assert tp[0] == "1"
    assert tp[1] == []


def test_scan_py_dict_struct_format(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    df = pd.DataFrame({"id": [1, 3, 4], "dt": [{"key1": 5, "key3": 4}, {"key1": 10, "key3": 25}, None]})
    res = conn.execute("LOAD FROM df RETURN *")
    tp = res.get_next()
    assert tp[0] == 1
    assert tp[1] == {"key1": 5, "key3": 4}
    tp = res.get_next()
    assert tp[0] == 3
    assert tp[1] == {"key1": 10, "key3": 25}
    tp = res.get_next()
    assert tp[0] == 4
    assert tp[1] is None


def test_scan_py_dict_map_format(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    df = pd.DataFrame({
        "id": [1, 3, 4],
        "dt": [
            {"key": ["Alice", "Bob"], "value": [32, 41]},
            {"key": ["Carol"], "value": [2]},
            {"key": ["zoo", "ela", "dan"], "value": [44, 52, 88]},
        ],
    })
    res = conn.execute("LOAD FROM df RETURN *")
    tp = res.get_next()
    assert tp[0] == 1
    assert tp[1] == {"Alice": 32, "Bob": 41}
    tp = res.get_next()
    assert tp[0] == 3
    assert tp[1] == {"Carol": 2}
    tp = res.get_next()
    assert tp[0] == 4
    assert tp[1] == {"zoo": 44, "ela": 52, "dan": 88}

    # If key and value size don't match, kuzu sniffs it as struct.
    df = pd.DataFrame({"id": [4], "dt": [{"key": ["Alice", "Bob"], "value": []}]})
    res = conn.execute("LOAD FROM df RETURN *")
    tup = res.get_next()
    assert tup[0] == 4
    assert tup[1] == {"key": ["Alice", "Bob"], "value": []}


def test_scan_py_dict_empty(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    df = pd.DataFrame({"id": [], "dt": []})
    res = conn.execute("LOAD FROM df RETURN *")
    assert not res.has_next()
