from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

import ground_truth
import kuzu
import polars as pl
import pyarrow as pa
import pytz
from pandas import Timestamp
from type_aliases import ConnDB

# enable polars' decimal support
pl.Config.activate_decimals(True)


_expected_dtypes = {
    # ------------------------------------------------
    # person
    # ------------------------------------------------
    "a.ID": {"arrow": pa.int64(), "pl": pl.Int64},
    "a.fName": {"arrow": pa.string(), "pl": pl.String},
    "a.gender": {"arrow": pa.int64(), "pl": pl.Int64},
    "a.isStudent": {"arrow": pa.bool_(), "pl": pl.Boolean},
    "a.isWorker": {"arrow": pa.bool_(), "pl": pl.Boolean},
    "a.age": {"arrow": pa.int64(), "pl": pl.Int64},
    "a.eyeSight": {"arrow": pa.float64(), "pl": pl.Float64},
    "a.birthdate": {"arrow": pa.date32(), "pl": pl.Date},
    "a.registerTime": {"arrow": pa.timestamp("us"), "pl": pl.Datetime("us")},
    "a.lastJobDuration": {"arrow": pa.duration("ms"), "pl": pl.Duration("ms")},
    "a.workedHours": {"arrow": pa.list_(pa.int64()), "pl": pl.List(pl.Int64)},
    "a.usedNames": {"arrow": pa.list_(pa.string()), "pl": pl.List(pl.String)},
    "a.courseScoresPerTerm": {"arrow": pa.list_(pa.list_(pa.int64())), "pl": pl.List(pl.List(pl.Int64))},
    "a.grades": {"arrow": pa.list_(pa.int64(), 4), "pl": pl.Array(pl.Int64, width=4)},
    "a.height": {"arrow": pa.float32(), "pl": pl.Float32},
    "a.u": {"arrow": pa.string(), "pl": pl.String},
    # ------------------------------------------------
    # movies
    # ------------------------------------------------
    "a.length": {"arrow": pa.int32(), "pl": pl.Int32},
    "m.name": {"arrow": pa.string(), "pl": pl.String},
    "a.description": {
        "arrow": pa.struct([
            ("rating", pa.float64()),
            ("stars", pa.int8()),
            ("views", pa.int64()),
            ("release", pa.timestamp("us")),
            ("release_ns", pa.timestamp("ns")),
            ("release_ms", pa.timestamp("ms")),
            ("release_sec", pa.timestamp("s")),
            ("release_tz", pa.timestamp("us", tz="UTC")),
            ("film", pa.date32()),
            ("u8", pa.uint8()),
            ("u16", pa.uint16()),
            ("u32", pa.uint32()),
            ("u64", pa.uint64()),
            ("hugedata", pa.decimal128(38, 0)),
        ]),
        "pl": pl.Struct({
            "rating": pl.Float64,
            "stars": pl.Int8,
            "views": pl.Int64,
            "release": pl.Datetime(time_unit="us"),
            "release_ns": pl.Datetime(time_unit="ns"),
            "release_ms": pl.Datetime(time_unit="ms"),
            "release_sec": pl.Datetime(time_unit="ms"),
            "release_tz": pl.Datetime(time_unit="us", time_zone="UTC"),
            "film": pl.Date,
            "u8": pl.UInt8,
            "u16": pl.UInt16,
            "u32": pl.UInt32,
            "u64": pl.UInt64,
            "hugedata": pl.Decimal(precision=38, scale=0),
        }),
    },
    # ------------------------------------------------
    # miscellaneous
    # ------------------------------------------------
    "a.lbl": {"arrow": pa.string(), "pl": pl.String},
    "a.orgCode": {"arrow": pa.int64(), "pl": pl.Int64},
}


def get_result(query_result: kuzu.QueryResult, result_type: str, chunk_size: int | None) -> Any:
    sz = [] if (chunk_size is None or result_type == "pl") else [chunk_size]
    res = getattr(query_result, f"get_as_{result_type}")(*sz)
    if result_type == "arrow" and chunk_size:
        assert res[0].num_chunks == max((len(res) // chunk_size), 1)
    return res


def assert_column_equals(data: Any, col_name: str, return_type: str, expected_values: list[Any]) -> None:
    col = data[col_name]
    col_dtype = col.dtype if hasattr(col, "dtype") else col.type
    values = col.to_pylist() if return_type == "arrow" else col.to_list()

    assert len(col) == len(expected_values)
    assert values == expected_values, f"Unexpected values for {col_name} ({return_type!r})"
    assert col_dtype == _expected_dtypes[col_name][return_type], f"Unexpected dtype for {col_name} ({return_type!r})"


def assert_col_names(data: Any, expected_col_names: list[str]) -> None:
    col_names = [(c._name if hasattr(c, "_name") else c.name) for c in data]
    assert col_names == expected_col_names, f"Unexpected column names: {col_names!r}"


def test_to_arrow(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly

    def _test_person_table(_conn: kuzu.Connection, return_type: str, chunk_size: int | None = None) -> None:
        query = "MATCH (a:person) RETURN a.* ORDER BY a.ID"
        data = get_result(_conn.execute(query), return_type, chunk_size)
        assert len(data.columns) == 16

        assert_column_equals(
            data=data,
            col_name="a.ID",
            return_type=return_type,
            expected_values=[0, 2, 3, 5, 7, 8, 9, 10],
        )

        assert_column_equals(
            data=data,
            col_name="a.fName",
            return_type=return_type,
            expected_values=[
                "Alice",
                "Bob",
                "Carol",
                "Dan",
                "Elizabeth",
                "Farooq",
                "Greg",
                "Hubert Blaine Wolfeschlegelsteinhausenbergerdorff",
            ],
        )

        assert_column_equals(
            data=data,
            col_name="a.gender",
            return_type=return_type,
            expected_values=[1, 2, 1, 2, 1, 2, 2, 2],
        )

        assert_column_equals(
            data=data,
            col_name="a.isStudent",
            return_type=return_type,
            expected_values=[True, True, False, False, False, True, False, False],
        )

        assert_column_equals(
            data=data,
            col_name="a.isWorker",
            return_type=return_type,
            expected_values=[False, False, True, True, True, False, False, True],
        )

        assert_column_equals(
            data=data,
            col_name="a.age",
            return_type=return_type,
            expected_values=[35, 30, 45, 20, 20, 25, 40, 83],
        )

        assert_column_equals(
            data=data,
            col_name="a.eyeSight",
            return_type=return_type,
            expected_values=[5.0, 5.1, 5.0, 4.8, 4.7, 4.5, 4.9, 4.9],
        )

        assert_column_equals(
            data=data,
            col_name="a.birthdate",
            return_type=return_type,
            expected_values=[
                date(1900, 1, 1),
                date(1900, 1, 1),
                date(1940, 6, 22),
                date(1950, 7, 23),
                date(1980, 10, 26),
                date(1980, 10, 26),
                date(1980, 10, 26),
                date(1990, 11, 27),
            ],
        )

        assert_column_equals(
            data=data,
            col_name="a.registerTime",
            return_type=return_type,
            expected_values=[
                datetime(2011, 8, 20, 11, 25, 30),
                datetime(2008, 11, 3, 15, 25, 30, 526),
                datetime(1911, 8, 20, 2, 32, 21),
                datetime(2031, 11, 30, 12, 25, 30),
                datetime(1976, 12, 23, 11, 21, 42),
                datetime(1972, 7, 31, 13, 22, 30, 678559),
                datetime(1976, 12, 23, 4, 41, 42),
                datetime(2023, 2, 21, 13, 25, 30),
            ],
        )

        assert_column_equals(
            data=data,
            col_name="a.lastJobDuration",
            return_type=return_type,
            expected_values=[
                timedelta(days=99, seconds=36334, microseconds=628000),
                timedelta(days=543, seconds=4800),
                timedelta(microseconds=125000),
                timedelta(days=541, seconds=57600, microseconds=24000),
                timedelta(0),
                timedelta(days=2016, seconds=68600),
                timedelta(microseconds=125000),
                timedelta(days=541, seconds=57600, microseconds=24000),
            ],
        )

        assert_column_equals(
            data=data,
            col_name="a.workedHours",
            return_type=return_type,
            expected_values=[[10, 5], [12, 8], [4, 5], [1, 9], [2], [3, 4, 5, 6, 7], [1], [10, 11, 12, 3, 4, 5, 6, 7]],
        )

        assert_column_equals(
            data=data,
            col_name="a.usedNames",
            return_type=return_type,
            expected_values=[
                ["Aida"],
                ["Bobby"],
                ["Carmen", "Fred"],
                ["Wolfeschlegelstein", "Daniel"],
                ["Ein"],
                ["Fesdwe"],
                ["Grad"],
                ["Ad", "De", "Hi", "Kye", "Orlan"],
            ],
        )

        assert_column_equals(
            data=data,
            col_name="a.courseScoresPerTerm",
            return_type=return_type,
            expected_values=[
                [[10, 8], [6, 7, 8]],
                [[8, 9], [9, 10]],
                [[8, 10]],
                [[7, 4], [8, 8], [9]],
                [[6], [7], [8]],
                [[8]],
                [[10]],
                [[7], [10], [6, 7]],
            ],
        )

        assert_column_equals(
            data=data,
            col_name="a.grades",
            return_type=return_type,
            expected_values=[
                [96, 54, 86, 92],
                [98, 42, 93, 88],
                [91, 75, 21, 95],
                [76, 88, 99, 89],
                [96, 59, 65, 88],
                [80, 78, 34, 83],
                [43, 83, 67, 43],
                [77, 64, 100, 54],
            ],
        )

        assert_column_equals(
            data=data,
            col_name="a.u",
            return_type=return_type,
            expected_values=[
                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12",
                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13",
                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a14",
                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a15",
                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a16",
                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a17",
                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a18",
            ],
        )

    def _test_movies_table(_conn: kuzu.Connection, return_type: str, chunk_size: int | None = None) -> None:
        query = "MATCH (a:movies) RETURN a.length, a.description ORDER BY a.length"
        data = get_result(_conn.execute(query), return_type, chunk_size)

        assert_col_names(data, ["a.length", "a.description"])
        assert_column_equals(
            data=data,
            col_name="a.length",
            return_type=return_type,
            expected_values=[126, 298, 2544],
        )

        assert_column_equals(
            data=data,
            col_name="a.description",
            return_type=return_type,
            expected_values=[
                {
                    "rating": 5.3,
                    "stars": 2,
                    "views": 152,
                    "release": datetime(2011, 8, 20, 11, 25, 30),
                    "release_ns": datetime(2011, 8, 20, 11, 25, 30, 123456)
                    if return_type == "pl"
                    else Timestamp("2011-08-20 11:25:30.123456"),
                    "release_ms": datetime(2011, 8, 20, 11, 25, 30, 123000),
                    "release_sec": datetime(2011, 8, 20, 11, 25, 30),
                    "release_tz": datetime(2011, 8, 20, 11, 25, 30, 123456, pytz.UTC),
                    "film": date(2012, 5, 11),
                    "u8": 220,
                    "u16": 20,
                    "u32": 1,
                    "u64": 180,
                    "hugedata": Decimal(1844674407370955161811111111),
                },
                {
                    "rating": 1223.0,
                    "stars": 100,
                    "views": 10003,
                    "release": datetime(2011, 2, 11, 16, 44, 22),
                    "release_ns": datetime(2011, 2, 11, 16, 44, 22, 123456)
                    if return_type == "pl"
                    else Timestamp("2011-02-11 16:44:22.123456"),
                    "release_ms": datetime(2011, 2, 11, 16, 44, 22, 123000),
                    "release_sec": datetime(2011, 2, 11, 16, 44, 22),
                    "release_tz": datetime(2011, 2, 11, 16, 44, 22, 123456, pytz.UTC),
                    "film": date(2013, 2, 22),
                    "u8": 1,
                    "u16": 15,
                    "u32": 200,
                    "u64": 4,
                    "hugedata": Decimal(-15),
                },
                {
                    "rating": 7.0,
                    "stars": 10,
                    "views": 982,
                    "release": datetime(2018, 11, 13, 13, 33, 11),
                    "release_ns": datetime(2018, 11, 13, 13, 33, 11, 123456)
                    if return_type == "pl"
                    else Timestamp("2018-11-13 13:33:11.123456"),
                    "release_ms": datetime(2018, 11, 13, 13, 33, 11, 123000),
                    "release_sec": datetime(2018, 11, 13, 13, 33, 11),
                    "release_tz": datetime(2018, 11, 13, 13, 33, 11, 123456, pytz.UTC),
                    "film": date(2014, 9, 12),
                    "u8": 12,
                    "u16": 120,
                    "u32": 55,
                    "u64": 1,
                    "hugedata": Decimal(-1844674407370955161511),
                },
            ],
        )

    def _test_utf8_string(_conn: kuzu.Connection, return_type: str, chunk_size: int | None = None) -> None:
        query = "MATCH (m:movies) RETURN m.name"
        data = get_result(_conn.execute(query), return_type, chunk_size)

        assert_col_names(data, ["m.name"])
        assert_column_equals(
            data=data,
            col_name="m.name",
            return_type=return_type,
            expected_values=["Sóló cón tu párejâ", "The 😂😃🧘🏻‍♂️🌍🌦️🍞🚗 movie", "Roma"],
        )

    def _test_in_small_chunk_size(_conn: kuzu.Connection, return_type: str, chunk_size: int | None = None) -> None:
        query = "MATCH (a:person) RETURN a.age, a.fName ORDER BY a.ID"
        data = get_result(_conn.execute(query), return_type, chunk_size)

        assert_col_names(data, ["a.age", "a.fName"])
        assert_column_equals(
            data=data,
            col_name="a.age",
            return_type=return_type,
            expected_values=[35, 30, 45, 20, 20, 25, 40, 83],
        )

        assert_column_equals(
            data=data,
            col_name="a.fName",
            return_type=return_type,
            expected_values=[
                "Alice",
                "Bob",
                "Carol",
                "Dan",
                "Elizabeth",
                "Farooq",
                "Greg",
                "Hubert Blaine Wolfeschlegelsteinhausenbergerdorff",
            ],
        )

    def _test_with_nulls(_conn: kuzu.Connection, return_type: str, chunk_size: int | None = None) -> None:
        query = "MATCH (a:person:organisation) RETURN label(a) AS `a.lbl`, a.fName, a.orgCode ORDER BY a.ID"
        data = get_result(_conn.execute(query), return_type, chunk_size)

        assert_col_names(data, ["a.lbl", "a.fName", "a.orgCode"])
        assert_column_equals(
            data=data,
            col_name="a.lbl",
            return_type=return_type,
            expected_values=[
                "person",
                "organisation",
                "person",
                "person",
                "organisation",
                "person",
                "organisation",
                "person",
                "person",
                "person",
                "person",
            ],
        )

        assert_column_equals(
            data=data,
            col_name="a.fName",
            return_type=return_type,
            expected_values=[
                "Alice",
                None,
                "Bob",
                "Carol",
                None,
                "Dan",
                None,
                "Elizabeth",
                "Farooq",
                "Greg",
                "Hubert Blaine Wolfeschlegelsteinhausenbergerdorff",
            ],
        )

        assert_column_equals(
            data=data,
            col_name="a.orgCode",
            return_type=return_type,
            expected_values=[None, 325, None, None, 934, None, 824, None, None, None, None],
        )

    _test_person_table(conn, "arrow", 9)
    _test_person_table(conn, "pl")
    _test_movies_table(conn, "arrow", 8)
    _test_movies_table(conn, "pl")
    _test_utf8_string(conn, "arrow", 3)
    _test_utf8_string(conn, "pl")
    _test_in_small_chunk_size(conn, "arrow", 4)
    _test_in_small_chunk_size(conn, "pl", 4)
    _test_with_nulls(conn, "arrow", 12)
    _test_with_nulls(conn, "pl")


def test_to_arrow_complex(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly

    def _test_node(_conn: kuzu.Connection) -> None:
        query = "MATCH (p:person) RETURN p ORDER BY p.ID"
        query_result = _conn.execute(query)
        arrow_tbl = query_result.get_as_arrow()
        p_col = arrow_tbl.column(0)

        assert p_col.to_pylist() == [
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[0],
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[2],
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[3],
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[5],
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[7],
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[8],
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[9],
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[10],
        ]

    def _test_node_rel(_conn: kuzu.Connection) -> None:
        query = "MATCH (a:person)-[e:workAt]->(b:organisation) RETURN a, e, b;"
        query_result = _conn.execute(query)
        arrow_tbl = query_result.get_as_arrow(0)
        assert arrow_tbl.num_columns == 3
        a_col = arrow_tbl.column(0)
        assert len(a_col) == 3
        e_col = arrow_tbl.column(1)
        assert len(a_col) == 3
        b_col = arrow_tbl.column(2)
        assert len(a_col) == 3
        assert a_col.to_pylist() == [
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[3],
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[5],
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[7],
        ]
        assert e_col.to_pylist() == [
            {
                "_src": {"offset": 2, "tableID": 0},
                "_dst": {"offset": 1, "tableID": 2},
                "_id": {"offset": 0, "tableID": 4},
                "year": 2015,
            },
            {
                "_src": {"offset": 3, "tableID": 0},
                "_dst": {"offset": 2, "tableID": 2},
                "_id": {"offset": 1, "tableID": 4},
                "year": 2010,
            },
            {
                "_src": {"offset": 4, "tableID": 0},
                "_dst": {"offset": 2, "tableID": 2},
                "_id": {"offset": 2, "tableID": 4},
                "year": 2015,
            },
        ]
        assert b_col.to_pylist() == [
            ground_truth.TINY_SNB_ORGANISATIONS_GROUND_TRUTH[4],
            ground_truth.TINY_SNB_ORGANISATIONS_GROUND_TRUTH[6],
            ground_truth.TINY_SNB_ORGANISATIONS_GROUND_TRUTH[6],
        ]

    def _test_marries_table(_conn: kuzu.Connection) -> None:
        query = "MATCH (:person)-[e:marries]->(:person) RETURN e.*"
        arrow_tbl = _conn.execute(query).get_as_arrow(0)
        assert arrow_tbl.num_columns == 3

        used_addr_col = arrow_tbl.column(0)
        assert used_addr_col.type == pa.list_(pa.string())
        assert len(used_addr_col) == 3
        assert used_addr_col.to_pylist() == [["toronto"], None, []]

        arrow_tbl.column(1)
        assert used_addr_col.type == pa.list_(pa.int16(), 2)
        assert len(used_addr_col) == 3
        assert used_addr_col.to_pylist() == [[4, 5], [2, 5], [3, 9]]

        arrow_tbl.column(2)
        assert used_addr_col.type == pa.string()
        assert len(used_addr_col) == 3
        assert used_addr_col.to_pylist() == [None, "long long long string", "short str"]

    # _test_node(conn)
    # _test_node_rel(conn)
    # _test_marries_table(conn)

    def test_to_arrow1(conn_db_readonly: ConnDB) -> None:
        conn, db = conn_db_readonly
        query = "MATCH (a:person)-[e:knows]->(:person) RETURN e.summary"
        arrow_tbl = conn.execute(query).get_as_arrow(-1)
        assert arrow_tbl == []
