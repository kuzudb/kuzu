from __future__ import annotations

import shutil
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from test_helper import KUZU_ROOT

python_build_dir = Path(__file__).parent.parent / "build"
try:
    import kuzu
except ModuleNotFoundError:
    sys.path.append(str(python_build_dir))
    import kuzu

if TYPE_CHECKING:
    from type_aliases import ConnDB


def init_npy(conn: kuzu.Connection) -> None:
    conn.execute(
        """
        CREATE NODE TABLE npyoned (
          i64 INT64,
          i32 INT32,
          i16 INT16,
          f64 DOUBLE,
          f32 FLOAT,
          PRIMARY KEY(i64)
        );
        """
    )
    conn.execute(
        f"""
        COPY npyoned from (
          "{KUZU_ROOT}/dataset/npy-1d/one_dim_int64.npy",
          "{KUZU_ROOT}/dataset/npy-1d/one_dim_int32.npy",
          "{KUZU_ROOT}/dataset/npy-1d/one_dim_int16.npy",
          "{KUZU_ROOT}/dataset/npy-1d/one_dim_double.npy",
          "{KUZU_ROOT}/dataset/npy-1d/one_dim_float.npy") BY COLUMN;
        """
    )
    conn.execute(
        """
        CREATE NODE TABLE npytwod (
          id INT64,
          i64 INT64[3],
          i32 INT32[3],
          i16 INT16[3],
          f64 DOUBLE[3],
          f32 FLOAT[3],
          PRIMARY KEY(id)
        );
        """
    )
    conn.execute(
        f"""
        COPY npytwod FROM (
          "{KUZU_ROOT}/dataset/npy-2d/id_int64.npy",
          "{KUZU_ROOT}/dataset/npy-2d/two_dim_int64.npy",
          "{KUZU_ROOT}/dataset/npy-2d/two_dim_int32.npy",
          "{KUZU_ROOT}/dataset/npy-2d/two_dim_int16.npy",
          "{KUZU_ROOT}/dataset/npy-2d/two_dim_double.npy",
          "{KUZU_ROOT}/dataset/npy-2d/two_dim_float.npy") BY COLUMN;
        """
    )


def init_tensor(conn: kuzu.Connection) -> None:
    conn.execute(
        """
        CREATE NODE TABLE tensor (
          ID INT64,
          boolTensor BOOLEAN[],
          doubleTensor DOUBLE[][],
          intTensor INT64[][][],
          oneDimInt INT64,
          PRIMARY KEY (ID)
        );
        """
    )
    conn.execute(f'COPY tensor FROM "{KUZU_ROOT}/dataset/tensor-list/vTensor.csv" (HEADER=true)')


def init_long_str(conn: kuzu.Connection) -> None:
    conn.execute("CREATE NODE TABLE personLongString (name STRING, spouse STRING, PRIMARY KEY(name))")
    conn.execute(f'COPY personLongString FROM "{KUZU_ROOT}/dataset/long-string-pk-tests/vPerson.csv"')
    conn.execute("CREATE REL TABLE knowsLongString (FROM personLongString TO personLongString, MANY_MANY)")
    conn.execute(f'COPY knowsLongString FROM "{KUZU_ROOT}/dataset/long-string-pk-tests/eKnows.csv"')


def init_tinysnb(conn: kuzu.Connection) -> None:
    tiny_snb_path = (Path(__file__).parent / f"{KUZU_ROOT}/dataset/tinysnb").resolve()
    schema_path = tiny_snb_path / "schema.cypher"
    with schema_path.open(mode="r") as f:
        for line in f.readlines():
            line = line.strip()
            if line:
                conn.execute(line)

    copy_path = tiny_snb_path / "copy.cypher"
    with copy_path.open(mode="r") as f:
        for line in f.readlines():
            line = line.strip()
            line = line.replace("dataset/tinysnb", f"{KUZU_ROOT}/dataset/tinysnb")
            if line:
                conn.execute(line)


def init_movie_serial(conn: kuzu.Connection) -> None:
    conn.execute(
        """
        CREATE NODE TABLE moviesSerial (
          ID SERIAL,
          name STRING,
          length INT32,
          note STRING,
          PRIMARY KEY (ID)
        );"""
    )
    conn.execute(f'COPY moviesSerial from "{KUZU_ROOT}/dataset/tinysnb-serial/vMovies.csv"')


def init_rdf(conn: kuzu.Connection) -> None:
    rdf_path = (Path(__file__).parent / f"{KUZU_ROOT}/dataset/rdf/rdf_variant").resolve()
    scripts = [Path(rdf_path) / "schema.cypher", Path(rdf_path) / "copy.cypher"]
    for script in scripts:
        with script.open(mode="r") as f:
            for line in f.readlines():
                if line := line.strip():
                    conn.execute(line)


def init_db(path: Path) -> Path:
    if Path(path).exists():
        shutil.rmtree(path)

    db = kuzu.Database(path)
    conn = kuzu.Connection(db)
    init_tinysnb(conn)
    init_npy(conn)
    init_tensor(conn)
    init_long_str(conn)
    init_movie_serial(conn)
    init_rdf(conn)
    return path


_READONLY_CONN_DB_: ConnDB | None = None
_POOL_SIZE_: int = 256 * 1024 * 1024


def _create_conn_db(path: Path, *, read_only: bool) -> ConnDB:
    """Return a new connection and database."""
    db = kuzu.Database(init_db(path), buffer_pool_size=_POOL_SIZE_, read_only=read_only)
    conn = kuzu.Connection(db, num_threads=4)
    return conn, db


@pytest.fixture()
def conn_db_readonly(tmp_path: Path) -> ConnDB:
    """Return a cached read-only connection and database."""
    global _READONLY_CONN_DB_
    if _READONLY_CONN_DB_ is None:
        # On Windows, the read-only mode is not implemented yet.
        # Therefore, we create a writable database on Windows.
        # However, the caller should ensure that the database is not modified.
        # TODO: Remove this workaround when the read-only mode is implemented on Windows.
        _READONLY_CONN_DB_ = _create_conn_db(tmp_path, read_only=sys.platform != "win32")
    return _READONLY_CONN_DB_


@pytest.fixture()
def conn_db_readwrite(tmp_path: Path) -> ConnDB:
    """Return a new writable connection and database."""
    return _create_conn_db(tmp_path, read_only=False)


@pytest.fixture()
def build_dir() -> Path:
    return python_build_dir
