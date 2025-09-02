from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING

import kuzu
import pytest
from conftest import get_db_file_path


def open_database_on_subprocess(tmp_path: Path, build_dir: Path) -> None:
    code = dedent(
        f"""
        import sys
        sys.path.append(r"{build_dir!s}")

        import kuzu
        db = kuzu.Database(r"{tmp_path!s}")
        print(r"{tmp_path!s}")
    """
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr)


def test_database_close(tmp_path: Path, build_dir: Path) -> None:
    db_path = tmp_path / "test_database_close.kuzu"
    db = kuzu.Database(database_path=db_path, read_only=False)
    assert not db.is_closed
    assert db._database is not None

    # Open the database on a subprocess. It should raise an exception.
    with pytest.raises(
        Exception,
        match=r"RuntimeError: IO exception: Could not set lock on file",
    ):
        open_database_on_subprocess(db_path, build_dir)

    db.close()
    assert db.is_closed
    assert db._database is None

    # Try to close the database again. It should not raise any exceptions.
    db.close()
    assert db.is_closed
    assert db._database is None

    # Open the database on a subprocess. It should not raise any exceptions.
    # TODO: Enable this test on Windows when the read-only mode is implemented.
    if sys.platform != "win32":
        open_database_on_subprocess(db_path, build_dir)


def test_database_context_manager(tmp_path: Path, build_dir: Path) -> None:
    db_path = tmp_path / "test_database_context_manager.kuzu"
    with kuzu.Database(database_path=db_path, read_only=False) as db:
        assert not db.is_closed
        assert db._database is not None

        # Open the database on a subprocess. It should raise an exception.
        # TODO: Enable this test on Windows when the read-only mode is implemented.
        if sys.platform != "win32":
            with pytest.raises(
                RuntimeError,
                match=r"RuntimeError: IO exception: Could not set lock on file",
            ):
                open_database_on_subprocess(db_path, build_dir)

    # Open the database on a subprocess. It should not raise any exceptions.
    # TODO: Enable this test on Windows when the read-only mode is implemented.
    if sys.platform != "win32":
        open_database_on_subprocess(db_path, build_dir)


def test_in_mem_database_memory_db_path() -> None:
    db = kuzu.Database(database_path=":memory:")
    assert not db.is_closed
    assert db._database is not None

    # Open the database on a subprocess. It should raise an exception.
    conn = kuzu.Connection(db)
    conn.execute("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
    conn.execute("CREATE (:person {name: 'Alice', age: 30});")
    conn.execute("CREATE (:person {name: 'Bob', age: 40});")
    result = conn.execute("MATCH (p:person) RETURN p.*")
    assert result.get_num_tuples() == 2


def test_in_mem_database_empty_db_path() -> None:
    db = kuzu.Database()
    assert not db.is_closed
    assert db._database is not None

    # Open the database on a subprocess. It should raise an exception.
    conn = kuzu.Connection(db)
    conn.execute("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
    conn.execute("CREATE (:person {name: 'Alice', age: 30});")
    conn.execute("CREATE (:person {name: 'Bob', age: 40});")
    result = conn.execute("MATCH (p:person) RETURN p.*")
    assert result.get_num_tuples() == 2


def test_in_mem_database_no_db_path() -> None:
    with kuzu.Database(database_path="") as db:
        assert not db.is_closed
        assert db._database is not None

        # Open the database on a subprocess. It should raise an exception.
        conn = kuzu.Connection(db)
        conn.execute("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
        conn.execute("CREATE (:person {name: 'Alice', age: 30});")
        conn.execute("CREATE (:person {name: 'Bob', age: 40});")
        with conn.execute("MATCH (p:person) RETURN p.*") as result:
            assert result.get_num_tuples() == 2


def test_database_auto_checkpoint_config(tmp_path: Path) -> None:
    with kuzu.Database(database_path=get_db_file_path(tmp_path), auto_checkpoint=False) as db:
        assert not db.is_closed
        assert db._database is not None

        conn = kuzu.Connection(db)
        with conn.execute("CALL current_setting('auto_checkpoint') RETURN *") as result:
            assert result.get_num_tuples() == 1
            assert result.get_next()[0] == "False"


def test_database_checkpoint_threshold_config(tmp_path: Path) -> None:
    with kuzu.Database(database_path=get_db_file_path(tmp_path), checkpoint_threshold=1234) as db:
        assert not db.is_closed
        assert db._database is not None

        conn = kuzu.Connection(db)
        with conn.execute("CALL current_setting('checkpoint_threshold') RETURN *") as result:
            assert result.get_num_tuples() == 1
            assert result.get_next()[0] == "1234"


def test_database_throw_on_wal_replay_failure_config(tmp_path: Path) -> None:
    database_path = get_db_file_path(tmp_path)
    wal_file_path = str(database_path) + ".wal"
    with Path.open(wal_file_path, "w") as wal_file:
        wal_file.write("a" * 28)
    with kuzu.Database(database_path=database_path, throw_on_wal_replay_failure=False) as db:
        assert not db.is_closed
        assert db._database is not None

        conn = kuzu.Connection(db)
        with conn.execute("RETURN 1") as result:
            assert result.get_num_tuples() == 1
            assert result.get_next()[0] == 1


def test_database_enable_checksums_config(tmp_path: Path) -> None:
    database_path = get_db_file_path(tmp_path)
    # first construct database file with enable_checksums=True
    with kuzu.Database(database_path=database_path) as db:
        assert not db.is_closed
        assert db._database is not None
        conn = kuzu.Connection(db)

        # do some updates to leave a WAL
        conn.execute("call auto_checkpoint=false")
        conn.execute("call force_checkpoint_on_close=false")
        conn.execute("create node table test(id int64 primary key)")

    # running again with enable_checksums=False should give an error
    with pytest.raises(RuntimeError) as check:
        db = kuzu.Database(database_path=database_path, enable_checksums=False)


def test_database_close_order() -> None:
    in_mem_db = kuzu.Database(database_path=":memory:", buffer_pool_size=1024 * 1024 * 10)
    assert not in_mem_db.is_closed
    assert in_mem_db._database is not None

    in_mem_conn = kuzu.Connection(in_mem_db)
    assert not in_mem_conn.is_closed
    assert in_mem_conn._connection is not None

    query_result = in_mem_conn.execute("RETURN 1+1")
    assert not query_result.is_closed
    assert query_result._query_result is not None

    assert query_result.get_next()[0] == 2
    # Close the database first, it should not cause crashes or exceptions
    in_mem_db.close()

    # The query result and connection will be unusable after the database is closed.
    # But calling methods on them should raise exceptions instead of crashing.
    with pytest.raises(Exception, match="Database is closed"):
        in_mem_conn.execute("RETURN 1+1")

    with pytest.raises(Exception, match="the parent database is closed"):
        query_result.get_next()

    # Close the connection and query result, they should not raise any exceptions.
    in_mem_conn.close()
    query_result.close()


def test_database_close_order_with_multiple_statements() -> None:
    in_mem_db = kuzu.Database(database_path=":memory:", buffer_pool_size=1024 * 1024 * 10)
    assert not in_mem_db.is_closed
    assert in_mem_db._database is not None

    in_mem_conn = kuzu.Connection(in_mem_db)
    assert not in_mem_conn.is_closed
    assert in_mem_conn._connection is not None

    query_results = in_mem_conn.execute("RETURN 1+1; RETURN 2+2; RETURN 3+3;")
    for i, qr in enumerate(query_results):
        assert not qr.is_closed
        assert qr._query_result is not None
        assert qr.get_next()[0] == (i + 1) * 2
    # Close the database first, it should not cause crashes or exceptions
    in_mem_db.close()

    # The query result and connection will be unusable after the database is closed.
    # But calling methods on them should raise exceptions instead of crashing.
    with pytest.raises(Exception, match="Database is closed"):
        in_mem_conn.execute("RETURN 1+1")

    with pytest.raises(Exception, match="the parent database is closed"):
        query_results[0].get_next()

    # Close the connection and query result, they should not raise any exceptions.
    in_mem_conn.close()
    for qr in query_results:
        qr.close()
