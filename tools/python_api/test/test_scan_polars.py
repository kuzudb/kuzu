from datetime import datetime

import polars as pl
import pytest
from type_aliases import ConnDB


def test_polars_basic(conn_db_readwrite: ConnDB) -> None:
    conn, _ = conn_db_readwrite
    df = pl.DataFrame([
        pl.Series("col1", [1, 2], dtype=pl.Int64),
        pl.Series("col2", ["a", "b"], dtype=pl.String),
        pl.Series("col3", [1.0, 2.0], dtype=pl.Float64),
        pl.Series("col4", [datetime(2020, 5, 5), datetime(2000, 1, 1)], dtype=pl.Datetime),
    ])
    result = conn.execute("LOAD FROM df RETURN *").get_as_pl()
    equivalency = result == df
    assert equivalency["col1"].all()
    assert equivalency["col2"].all()
    assert equivalency["col3"].all()
    assert equivalency["col4"].all()
    assert result["col1"].dtype == pl.Int64
    assert result["col2"].dtype == pl.String
    assert result["col3"].dtype == pl.Float64
    assert result["col4"].dtype == pl.Datetime
    # since polars just uses arrow as its backend, it's probably not necessary to extensively test this functionality
    conn.execute(
        "CREATE NODE TABLE polarstab(col1 INT64, col2 STRING, col3 DOUBLE, col4 TIMESTAMP, PRIMARY KEY(col1))"
    )
    conn.execute("COPY polarstab FROM df")
    result = conn.execute(
        "MATCH (t:polarstab) RETURN t.col1 AS col1, t.col2 AS col2, t.col3 AS col3, t.col4 AS col4 ORDER BY col1"
    ).get_as_pl()
    equivalency = result == df
    assert equivalency["col1"].all()
    assert equivalency["col2"].all()
    assert equivalency["col3"].all()
    assert equivalency["col4"].all()
    assert result["col1"].dtype == pl.Int64
    assert result["col2"].dtype == pl.String
    assert result["col3"].dtype == pl.Float64
    assert result["col4"].dtype == pl.Datetime


def test_polars_basic_param(conn_db_readwrite: ConnDB) -> None:
    conn, _ = conn_db_readwrite
    df = pl.DataFrame([
        pl.Series("col1", [1, 2], dtype=pl.Int64),
        pl.Series("col2", ["a", "b"], dtype=pl.String),
        pl.Series("col3", [1.0, 2.0], dtype=pl.Float64),
        pl.Series("col4", [datetime(2020, 5, 5), datetime(2000, 1, 1)], dtype=pl.Datetime),
    ])
    result = conn.execute("LOAD FROM df RETURN *").get_as_pl()
    equivalency = result == df
    assert equivalency["col1"].all()
    assert equivalency["col2"].all()
    assert equivalency["col3"].all()
    assert equivalency["col4"].all()
    assert result["col1"].dtype == pl.Int64
    assert result["col2"].dtype == pl.String
    assert result["col3"].dtype == pl.Float64
    assert result["col4"].dtype == pl.Datetime
    # since polars just uses arrow as its backend, it's probably not necessary to extensively test this functionality
    conn.execute(
        "CREATE NODE TABLE polarstab(col1 INT64, col2 STRING, col3 DOUBLE, col4 TIMESTAMP, PRIMARY KEY(col1))"
    )
    conn.execute("COPY polarstab FROM $df", {"df": df})
    result = conn.execute(
        "MATCH (t:polarstab) RETURN t.col1 AS col1, t.col2 AS col2, t.col3 AS col3, t.col4 AS col4 ORDER BY col1"
    ).get_as_pl()
    equivalency = result == df
    assert equivalency["col1"].all()
    assert equivalency["col2"].all()
    assert equivalency["col3"].all()
    assert equivalency["col4"].all()
    assert result["col1"].dtype == pl.Int64
    assert result["col2"].dtype == pl.String
    assert result["col3"].dtype == pl.Float64
    assert result["col4"].dtype == pl.Datetime


def test_polars_error(conn_db_readonly: ConnDB) -> None:
    conn, _ = conn_db_readonly
    with pytest.raises(RuntimeError, match=r"Binder exception: Variable df is not in scope."):
        conn.execute("LOAD FROM df RETURN *;")
    df = []
    with pytest.raises(
        RuntimeError,
        match=r"Binder exception: Attempted to scan from unsupported python object. Can only scan from pandas/polars dataframes and pyarrow tables.",
    ):
        conn.execute("LOAD FROM df RETURN *;")


def test_polars_scan_ignore_errors(conn_db_readwrite: ConnDB) -> None:
    conn, _ = conn_db_readwrite
    df = pl.DataFrame({"id": [1, 2, 3, 1]})
    conn.execute("CREATE NODE TABLE ids(id INT64, PRIMARY KEY(id))")
    conn.execute("COPY ids FROM df(IGNORE_ERRORS=true)")

    people = conn.execute("MATCH (i:ids) RETURN i.id")
    assert people.get_next() == [1]
    assert people.get_next() == [2]
    assert people.get_next() == [3]
    assert not people.has_next()

    warnings = conn.execute("CALL show_warnings() RETURN *")
    assert warnings.get_next()[1].startswith("Found duplicated primary key value 1")
    assert not warnings.has_next()


def test_copy_from_polars_multi_pairs(conn_db_readwrite: ConnDB) -> None:
    conn, _ = conn_db_readwrite
    conn.execute("CREATE NODE TABLE prof(id INT64, PRIMARY KEY(id))")
    conn.execute("CREATE (p:prof {id: 3});")
    conn.execute("CREATE (p:prof {id: 4});")
    conn.execute("CREATE NODE TABLE student(id INT64, PRIMARY KEY(id))")
    conn.execute("CREATE (p:student {id: 2});")
    conn.execute("CREATE REL TABLE teaches(from prof to prof, from prof to student, length int64)")
    df = pl.DataFrame({"from": [3], "to": [4], "length": [252]})
    conn.execute("COPY teaches from df (from = 'prof', to = 'prof');")
    result = conn.execute("match (:prof)-[e:teaches]->(:prof) return e.*")
    assert result.has_next()
    assert result.get_next()[0] == 252
    assert not result.has_next()


def test_scan_from_empty_lst(conn_db_readwrite: ConnDB) -> None:
    conn, _ = conn_db_readwrite
    df = pl.DataFrame({"prop1": [3], "prop2": [[]]})
    result = conn.execute("LOAD FROM df RETURN *")
    assert result.has_next()
    tp = result.get_next()
    assert tp[0] == 3
    assert tp[1] == []
