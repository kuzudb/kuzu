from datetime import datetime

import polars as pl
import pytest
from type_aliases import ConnDB


def test_polars_basic(conn_db_readwrite: ConnDB) -> None:
    conn, db = conn_db_readwrite
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


def test_polars_error(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    with pytest.raises(RuntimeError, match="Binder exception: Variable df is not in scope."):
        conn.execute("LOAD FROM df RETURN *;")
    df = []
    with pytest.raises(RuntimeError, match="Binder exception: Variable df found but no matches were scannable"):
        conn.execute("LOAD FROM df RETURN *;")
