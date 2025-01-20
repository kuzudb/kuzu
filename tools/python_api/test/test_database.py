from __future__ import annotations

import subprocess
import sys
from textwrap import dedent
from typing import TYPE_CHECKING

import kuzu
import pytest

if TYPE_CHECKING:
    from pathlib import Path


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
    with kuzu.Database(database_path=tmp_path, auto_checkpoint=False) as db:
        assert not db.is_closed
        assert db._database is not None

        conn = kuzu.Connection(db)
        with conn.execute("CALL current_setting('auto_checkpoint') RETURN *") as result:
            assert result.get_num_tuples() == 1
            assert result.get_next()[0] == "False"


def test_database_checkpoint_threshold_config(tmp_path: Path) -> None:
    with kuzu.Database(database_path=tmp_path, checkpoint_threshold=1234) as db:
        assert not db.is_closed
        assert db._database is not None

        conn = kuzu.Connection(db)
        with conn.execute("CALL current_setting('checkpoint_threshold') RETURN *") as result:
            assert result.get_num_tuples() == 1
            assert result.get_next()[0] == "1234"
