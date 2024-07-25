from __future__ import annotations

import datetime
import math
from decimal import Decimal
from typing import Any
from uuid import UUID

import kuzu
import pytz
from pandas import Timedelta, Timestamp
from type_aliases import ConnDB


def test_to_df(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly

    def _test_person_to_df(conn: kuzu.Connection) -> None:
        query = "MATCH (p:person) return p.* ORDER BY p.ID"
        pd = conn.execute(query).get_as_df()
        assert pd["p.ID"].tolist() == [0, 2, 3, 5, 7, 8, 9, 10]
        assert str(pd["p.ID"].dtype) == "int64"
        assert pd["p.fName"].tolist() == [
            "Alice",
            "Bob",
            "Carol",
            "Dan",
            "Elizabeth",
            "Farooq",
            "Greg",
            "Hubert Blaine Wolfeschlegelsteinhausenbergerdorff",
        ]
        assert str(pd["p.fName"].dtype) == "object"
        assert pd["p.gender"].tolist() == [1, 2, 1, 2, 1, 2, 2, 2]
        assert str(pd["p.gender"].dtype) == "int64"
        assert pd["p.isStudent"].tolist() == [
            True,
            True,
            False,
            False,
            False,
            True,
            False,
            False,
        ]
        assert str(pd["p.isStudent"].dtype) == "bool"
        assert pd["p.eyeSight"].tolist() == [5.0, 5.1, 5.0, 4.8, 4.7, 4.5, 4.9, 4.9]
        assert str(pd["p.eyeSight"].dtype) == "float64"
        assert pd["p.birthdate"].tolist() == [
            Timestamp("1900-01-01"),
            Timestamp("1900-01-01"),
            Timestamp("1940-06-22"),
            Timestamp("1950-07-23 "),
            Timestamp("1980-10-26"),
            Timestamp("1980-10-26"),
            Timestamp("1980-10-26"),
            Timestamp("1990-11-27"),
        ]
        assert str(pd["p.birthdate"].dtype) == "datetime64[us]"
        assert pd["p.registerTime"].tolist() == [
            Timestamp("2011-08-20 11:25:30"),
            Timestamp("2008-11-03 15:25:30.000526"),
            Timestamp("1911-08-20 02:32:21"),
            Timestamp("2031-11-30 12:25:30"),
            Timestamp("1976-12-23 11:21:42"),
            Timestamp("1972-07-31 13:22:30.678559"),
            Timestamp("1976-12-23 04:41:42"),
            Timestamp("2023-02-21 13:25:30"),
        ]
        assert str(pd["p.registerTime"].dtype) == "datetime64[us]"
        assert pd["p.lastJobDuration"].tolist() == [
            Timedelta("1082 days 13:02:00"),
            Timedelta("3750 days 13:00:00.000024"),
            Timedelta("2 days 00:24:11"),
            Timedelta("3750 days 13:00:00.000024"),
            Timedelta("2 days 00:24:11"),
            Timedelta("0 days 00:18:00.024000"),
            Timedelta("3750 days 13:00:00.000024"),
            Timedelta("1082 days 13:02:00"),
        ]
        assert str(pd["p.lastJobDuration"].dtype) == "timedelta64[ns]"
        assert pd["p.workedHours"].tolist() == [
            [10, 5],
            [12, 8],
            [4, 5],
            [1, 9],
            [2],
            [3, 4, 5, 6, 7],
            [1],
            [10, 11, 12, 3, 4, 5, 6, 7],
        ]
        assert str(pd["p.workedHours"].dtype) == "object"
        assert pd["p.usedNames"].tolist() == [
            ["Aida"],
            ["Bobby"],
            ["Carmen", "Fred"],
            ["Wolfeschlegelstein", "Daniel"],
            ["Ein"],
            ["Fesdwe"],
            ["Grad"],
            ["Ad", "De", "Hi", "Kye", "Orlan"],
        ]
        assert str(pd["p.usedNames"].dtype) == "object"
        assert pd["p.courseScoresPerTerm"].tolist() == [
            [[10, 8], [6, 7, 8]],
            [[8, 9], [9, 10]],
            [[8, 10]],
            [[7, 4], [8, 8], [9]],
            [[6], [7], [8]],
            [[8]],
            [[10]],
            [[7], [10], [6, 7]],
        ]
        assert str(pd["p.courseScoresPerTerm"].dtype) == "object"
        assert pd["p.grades"].tolist() == [
            [96, 54, 86, 92],
            [98, 42, 93, 88],
            [91, 75, 21, 95],
            [76, 88, 99, 89],
            [96, 59, 65, 88],
            [80, 78, 34, 83],
            [43, 83, 67, 43],
            [77, 64, 100, 54],
        ]
        assert str(pd["p.grades"].dtype) == "object"
        expected_values = [1.731, 0.99, 1.00, 1.30, 1.463, 1.51, 1.6, 1.323]
        actual_values = pd["p.height"].tolist()
        for expected, actual in zip(expected_values, actual_values):
            assert math.isclose(actual, expected, rel_tol=1e-5)
        assert str(pd["p.height"].dtype) == "float32"

    def _test_study_at_to_df(conn: kuzu.Connection) -> None:
        query = "MATCH (p:person)-[r:studyAt]->(o:organisation) return r.* order by r.length;"
        pd = conn.execute(query).get_as_df()
        assert pd["r.year"].tolist() == [2021, 2020, 2020]
        assert str(pd["r.year"].dtype) == "int64"
        assert pd["r.places"].tolist() == [
            ["wwAewsdndweusd", "wek"],
            ["awndsnjwejwen", "isuhuwennjnuhuhuwewe"],
            ["anew", "jsdnwusklklklwewsd"],
        ]
        assert str(pd["r.places"].dtype) == "object"
        assert pd["r.length"].tolist() == [5, 22, 55]
        assert str(pd["r.length"].dtype) == "int16"
        assert pd["r.level"].tolist() == [5, 2, 120]
        assert str(pd["r.level"].dtype) == "int8"
        assert pd["r.code"].tolist() == [9223372036854775808, 23, 6689]
        assert str(pd["r.code"].dtype) == "uint64"
        assert pd["r.temperature"].tolist() == [32800, 20, 1]
        assert str(pd["r.temperature"].dtype) == "uint32"
        assert pd["r.ulength"].tolist() == [33768, 180, 90]
        assert str(pd["r.ulength"].dtype) == "uint16"
        assert pd["r.ulevel"].tolist() == [250, 12, 220]
        assert str(pd["r.ulevel"].dtype) == "uint8"
        assert pd["r.hugedata"].tolist() == [
            1.8446744073709552e27,
            -15.0,
            -1.8446744073709552e21,
        ]
        assert str(pd["r.hugedata"].dtype) == "float64"

    def _test_timestamps_to_df(conn: kuzu.Connection) -> None:
        query = (
            'RETURN cast("2012-01-01 11:12:12.12345", "TIMESTAMP_NS") as A, cast("2012-01-01 11:12:12.12345", '
            '"TIMESTAMP_MS") as B, cast("2012-01-01 11:12:12.12345", "TIMESTAMP_SEC") as C, '
            'cast("2012-01-01 11:12:12.12345", "TIMESTAMP_TZ") as D'
        )
        pd = conn.execute(query).get_as_df()
        assert pd["A"].tolist() == [Timestamp("2012-01-01 11:12:12.123450")]
        assert pd["B"].tolist() == [Timestamp("2012-01-01 11:12:12.123000")]
        assert pd["C"].tolist() == [Timestamp("2012-01-01 11:12:12")]
        assert pd["D"].tolist() == [Timestamp("2012-01-01 11:12:12.123450")]

    def _test_movies_to_df(conn: kuzu.Connection) -> None:
        query = "MATCH (m:movies) return m.* order by m.length;"
        pd = conn.execute(query).get_as_df()
        assert pd["m.length"].tolist() == [126, 298, 2544]
        assert str(pd["m.length"].dtype) == "int32"
        assert pd["m.description"].tolist() == [
            {
                "rating": 5.3,
                "stars": 2,
                "views": 152,
                "release": datetime.datetime(2011, 8, 20, 11, 25, 30),
                "release_ns": datetime.datetime(2011, 8, 20, 11, 25, 30, 123456),
                "release_ms": datetime.datetime(2011, 8, 20, 11, 25, 30, 123000),
                "release_sec": datetime.datetime(2011, 8, 20, 11, 25, 30),
                "release_tz": datetime.datetime(2011, 8, 20, 11, 25, 30, 123456, pytz.UTC),
                "film": datetime.date(2012, 5, 11),
                "u8": 220,
                "u16": 20,
                "u32": 1,
                "u64": 180,
                "hugedata": Decimal("1844674407370955161811111111"),
            },
            {
                "rating": 1223.0,
                "stars": 100,
                "views": 10003,
                "release": datetime.datetime(2011, 2, 11, 16, 44, 22),
                "release_ns": datetime.datetime(2011, 2, 11, 16, 44, 22, 123456),
                "release_ms": datetime.datetime(2011, 2, 11, 16, 44, 22, 123000),
                "release_sec": datetime.datetime(2011, 2, 11, 16, 44, 22),
                "release_tz": datetime.datetime(2011, 2, 11, 16, 44, 22, 123456, pytz.UTC),
                "film": datetime.date(2013, 2, 22),
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
                "release": datetime.datetime(2018, 11, 13, 13, 33, 11),
                "release_ns": datetime.datetime(2018, 11, 13, 13, 33, 11, 123456),
                "release_ms": datetime.datetime(2018, 11, 13, 13, 33, 11, 123000),
                "release_sec": datetime.datetime(2018, 11, 13, 13, 33, 11),
                "release_tz": datetime.datetime(2018, 11, 13, 13, 33, 11, 123456, pytz.UTC),
                "film": datetime.date(2014, 9, 12),
                "u8": 12,
                "u16": 120,
                "u32": 55,
                "u64": 1,
                "hugedata": Decimal(-1844674407370955161511),
            },
        ]
        assert str(pd["m.description"].dtype) == "object"
        assert pd["m.content"].tolist() == [
            b"\xaa\xabinteresting\x0b",
            b"pure ascii characters",
            b"\xab\xcd",
        ]
        assert str(pd["m.content"].dtype) == "object"
        assert pd["m.audience"].tolist() == [
            {"audience1": 52, "audience53": 42},
            {},
            {"audience1": 33},
        ]
        assert str(pd["m.audience"].dtype) == "object"
        assert pd["m.grade"].tolist() == [True, 254.0, 8.989]
        assert str(pd["m.grade"].dtype) == "object"

    def _test_serial_to_df(conn: kuzu.Connection) -> None:
        df = conn.execute("MATCH (a:moviesSerial) RETURN a.ID AS id").get_as_df()
        assert len(df) == 3
        assert df["id"].tolist() == [0, 1, 2]

    _test_person_to_df(conn)
    conn.set_max_threads_for_exec(2)
    _test_study_at_to_df(conn)
    _test_movies_to_df(conn)
    _test_timestamps_to_df(conn)
    _test_serial_to_df(conn)


def test_df_multiple_times(conn_db_readonly: ConnDB) -> None:
    conn, _ = conn_db_readonly
    query = "MATCH (p:person) return p.ID ORDER BY p.ID"
    res = conn.execute(query)
    df = res.get_as_df()
    df_2 = res.get_as_df()
    df_3 = res.get_as_df()
    assert df["p.ID"].tolist() == [0, 2, 3, 5, 7, 8, 9, 10]
    assert df_2["p.ID"].tolist() == [0, 2, 3, 5, 7, 8, 9, 10]
    assert df_3["p.ID"].tolist() == [0, 2, 3, 5, 7, 8, 9, 10]


def test_df_get_node(conn_db_readonly: ConnDB) -> None:
    conn, _ = conn_db_readonly
    query = "MATCH (p:person) return p"

    res = conn.execute(query)
    df = res.get_as_df()
    p_list = df["p"].tolist()
    assert len(p_list) == 8

    ground_truth: dict[str, list[Any]] = {
        "ID": [0, 2, 3, 5, 7, 8, 9, 10],
        "fName": [
            "Alice",
            "Bob",
            "Carol",
            "Dan",
            "Elizabeth",
            "Farooq",
            "Greg",
            "Hubert Blaine Wolfeschlegelsteinhausenbergerdorff",
        ],
        "gender": [1, 2, 1, 2, 1, 2, 2, 2],
        "isStudent": [True, True, False, False, False, True, False, False],
        "eyeSight": [5.0, 5.1, 5.0, 4.8, 4.7, 4.5, 4.9, 4.9],
        "birthdate": [
            datetime.date(1900, 1, 1),
            datetime.date(1900, 1, 1),
            datetime.date(1940, 6, 22),
            datetime.date(1950, 7, 23),
            datetime.date(1980, 10, 26),
            datetime.date(1980, 10, 26),
            datetime.date(1980, 10, 26),
            datetime.date(1990, 11, 27),
        ],
        "registerTime": [
            Timestamp("2011-08-20 11:25:30"),
            Timestamp("2008-11-03 15:25:30.000526"),
            Timestamp("1911-08-20 02:32:21"),
            Timestamp("2031-11-30 12:25:30"),
            Timestamp("1976-12-23 11:21:42"),
            Timestamp("1972-07-31 13:22:30.678559"),
            Timestamp("1976-12-23 04:41:42"),
            Timestamp("2023-02-21 13:25:30"),
        ],
        "lastJobDuration": [
            Timedelta("1082 days 13:02:00"),
            Timedelta("3750 days 13:00:00.000024"),
            Timedelta("2 days 00:24:11"),
            Timedelta("3750 days 13:00:00.000024"),
            Timedelta("2 days 00:24:11"),
            Timedelta("0 days 00:18:00.024000"),
            Timedelta("3750 days 13:00:00.000024"),
            Timedelta("1082 days 13:02:00"),
        ],
        "workedHours": [
            [10, 5],
            [12, 8],
            [4, 5],
            [1, 9],
            [2],
            [3, 4, 5, 6, 7],
            [1],
            [10, 11, 12, 3, 4, 5, 6, 7],
        ],
        "usedNames": [
            ["Aida"],
            ["Bobby"],
            ["Carmen", "Fred"],
            ["Wolfeschlegelstein", "Daniel"],
            ["Ein"],
            ["Fesdwe"],
            ["Grad"],
            ["Ad", "De", "Hi", "Kye", "Orlan"],
        ],
        "courseScoresPerTerm": [
            [[10, 8], [6, 7, 8]],
            [[8, 9], [9, 10]],
            [[8, 10]],
            [[7, 4], [8, 8], [9]],
            [[6], [7], [8]],
            [[8]],
            [[10]],
            [[7], [10], [6, 7]],
        ],
        "_label": [
            "person",
            "person",
            "person",
            "person",
            "person",
            "person",
            "person",
            "person",
        ],
    }
    for i in range(len(p_list)):
        p = p_list[i]
        for key in ground_truth:
            assert p[key] == ground_truth[key][i]


def test_df_get_node_rel(conn_db_readonly: ConnDB) -> None:
    conn, _ = conn_db_readonly
    res = conn.execute("MATCH (p:person)-[r:workAt]->(o:organisation) RETURN p, r, o ORDER BY p.fName")

    df = res.get_as_df()
    p_list = df["p"].tolist()
    o_list = df["o"].tolist()

    assert len(p_list) == 3
    assert len(o_list) == 3

    ground_truth_p: dict[str, list[Any]] = {
        "ID": [3, 5, 7],
        "fName": ["Carol", "Dan", "Elizabeth"],
        "gender": [1, 2, 1],
        "isStudent": [False, False, False],
        "eyeSight": [5.0, 4.8, 4.7],
        "birthdate": [
            datetime.date(1940, 6, 22),
            datetime.date(1950, 7, 23),
            datetime.date(1980, 10, 26),
        ],
        "registerTime": [
            Timestamp("1911-08-20 02:32:21"),
            Timestamp("2031-11-30 12:25:30"),
            Timestamp("1976-12-23 11:21:42"),
        ],
        "lastJobDuration": [
            Timedelta("48 hours 24 minutes 11 seconds"),
            Timedelta("3750 days 13:00:00.000024"),
            Timedelta("2 days 00:24:11"),
        ],
        "workedHours": [[4, 5], [1, 9], [2]],
        "usedNames": [["Carmen", "Fred"], ["Wolfeschlegelstein", "Daniel"], ["Ein"]],
        "courseScoresPerTerm": [[[8, 10]], [[7, 4], [8, 8], [9]], [[6], [7], [8]]],
        "_label": ["person", "person", "person"],
    }
    for i in range(len(p_list)):
        p = p_list[i]
        for key in ground_truth_p:
            assert p[key] == ground_truth_p[key][i]

    ground_truth_o: dict[str, list[Any]] = {
        "ID": [4, 6, 6],
        "name": ["CsWork", "DEsWork", "DEsWork"],
        "orgCode": [934, 824, 824],
        "mark": [4.1, 4.1, 4.1],
        "score": [-100, 7, 7],
        "history": [
            "2 years 4 days 10 hours",
            "2 years 4 hours 22 us 34 minutes",
            "2 years 4 hours 22 us 34 minutes",
        ],
        "licenseValidInterval": [
            Timedelta(days=9414),
            Timedelta(days=3, seconds=36000, microseconds=100000),
            Timedelta(days=3, seconds=36000, microseconds=100000),
        ],
        "rating": [0.78, 0.52, 0.52],
        "_label": ["organisation", "organisation", "organisation"],
    }
    for i in range(len(o_list)):
        o = df["o"][i]
        for key in ground_truth_o:
            assert o[key] == ground_truth_o[key][i]

    assert df["r"][0]["year"] == 2015
    assert df["r"][1]["year"] == 2010
    assert df["r"][2]["year"] == 2015

    for i in range(len(df["r"])):
        assert df["r"][i]["_src"] == df["p"][i]["_id"]
        assert df["r"][i]["_dst"] == df["o"][i]["_id"]


def test_df_get_recursive_join(conn_db_readonly: ConnDB) -> None:
    conn, _ = conn_db_readonly
    res = conn.execute(
        "MATCH (p:person)-[r:knows*1..2 (e, n | WHERE e.comments = ['rnme','m8sihsdnf2990nfiwf'])]-(m:person) WHERE "
        "p.ID = 0 and m.ID = 0 RETURN r"
    ).get_as_df()
    assert res["r"][0] == {
        "_nodes": [
            {
                "ID": 2,
                "_id": {"offset": 1, "table": 0},
                "_label": "person",
                "age": 30,
                "birthdate": datetime.date(1900, 1, 1),
                "courseScoresPerTerm": [[8, 9], [9, 10]],
                "eyeSight": 5.1,
                "fName": "Bob",
                "gender": 2,
                "grades": [98, 42, 93, 88],
                "height": 0.9900000095367432,
                "u": UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12"),
                "isStudent": True,
                "isWorker": False,
                "lastJobDuration": datetime.timedelta(days=3750, seconds=46800, microseconds=24),
                "registerTime": datetime.datetime(2008, 11, 3, 15, 25, 30, 526),
                "usedNames": ["Bobby"],
                "workedHours": [12, 8],
            }
        ],
        "_rels": [
            {
                "_dst": {"offset": 1, "table": 0},
                "_label": "knows",
                "_src": {"offset": 0, "table": 0},
                "comments": ["rnme", "m8sihsdnf2990nfiwf"],
                "date": datetime.date(2021, 6, 30),
                "meetTime": datetime.datetime(1986, 10, 21, 21, 8, 31, 521000),
                "notes": 1,
                "summary": {
                    "locations": ["'toronto'", "'waterloo'"],
                    "transfer": {
                        "amount": [100, 200],
                        "day": datetime.date(2021, 1, 2),
                    },
                },
                "someMap": {"a": "b"},
                "validInterval": datetime.timedelta(days=3750, seconds=46800, microseconds=24),
            },
            {
                "_dst": {"offset": 1, "table": 0},
                "_label": "knows",
                "_src": {"offset": 0, "table": 0},
                "comments": ["rnme", "m8sihsdnf2990nfiwf"],
                "date": datetime.date(2021, 6, 30),
                "meetTime": datetime.datetime(1986, 10, 21, 21, 8, 31, 521000),
                "notes": 1,
                "summary": {
                    "locations": ["'toronto'", "'waterloo'"],
                    "transfer": {
                        "amount": [100, 200],
                        "day": datetime.date(2021, 1, 2),
                    },
                },
                "someMap": {"a": "b"},
                "validInterval": datetime.timedelta(days=3750, seconds=46800, microseconds=24),
            },
        ],
    }


def test_get_rdf_variant(conn_db_readonly: ConnDB) -> None:
    conn, _ = conn_db_readonly
    res = conn.execute("MATCH (a:T_l) RETURN a.val ORDER BY a.id").get_as_df()
    assert res["a.val"].tolist() == [
        12,
        43,
        33,
        2,
        90,
        77,
        12,
        1,
        4.4,
        1.2000000476837158,
        True,
        "hhh",
        datetime.date(2024, 1, 1),
        datetime.datetime(2024, 1, 1, 11, 25, 30),
        datetime.timedelta(days=2),
        b"\xb2",
    ]


def test_get_df_unicode(conn_db_readonly: ConnDB) -> None:
    conn, _ = conn_db_readonly
    res = conn.execute("MATCH (m:movies) RETURN m.name").get_as_df()
    assert res["m.name"].tolist() == [
        "Sóló cón tu párejâ",
        "The 😂😃🧘🏻‍♂️🌍🌦️🍞🚗 movie",
        "Roma",
    ]

def test_get_df_decimal(conn_db_readonly: ConnDB) -> None:
    conn, _ = conn_db_readonly
    res = conn.execute("UNWIND [1, 2, 3] AS A UNWIND [5.7, 8.3, 2.9] AS B RETURN CAST(CAST(A AS DECIMAL) * CAST(B AS DECIMAL) AS DECIMAL(18, 1)) AS PROD").get_as_df()
    assert sorted(res["PROD"].tolist()) == sorted([
        Decimal('5.7'),
        Decimal('8.3'),
        Decimal('2.9'),
        Decimal('11.4'),
        Decimal('16.6'),
        Decimal('5.8'),
        Decimal('17.1'),
        Decimal('24.9'),
        Decimal('8.7'),
    ])
    res = conn.execute("UNWIND [1, 2, 3] AS A UNWIND [5.7, 8.3, 2.9] AS B RETURN CAST(CAST(A AS DECIMAL) * CAST(B AS DECIMAL) AS DECIMAL(4, 1)) AS PROD").get_as_df()
    assert sorted(res["PROD"].tolist()) == sorted([
        Decimal('5.7'),
        Decimal('8.3'),
        Decimal('2.9'),
        Decimal('11.4'),
        Decimal('16.6'),
        Decimal('5.8'),
        Decimal('17.1'),
        Decimal('24.9'),
        Decimal('8.7'),
    ])
