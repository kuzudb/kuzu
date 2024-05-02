from __future__ import annotations

import sys

import kuzu
import pytest
from type_aliases import ConnDB


def test_exception(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly

    with pytest.raises(RuntimeError, match="Parameter asd not found."):
        conn.execute("MATCH (a:person) WHERE a.registerTime = $1 RETURN COUNT(*);", {"asd": 1})

    with pytest.raises(RuntimeError, match="Binder exception: Cannot find property dummy for a."):
        conn.execute("MATCH (a:person) RETURN a.dummy;")


def test_db_path_exception() -> None:
    path = ""
    error_message = (
        "IO exception: Failed to create directory  due to: IO exception: Directory  cannot be created. "
        "Check if it exists and remove it."
    )
    with pytest.raises(RuntimeError, match=error_message):
        kuzu.Database(path)


def test_read_only_exception(conn_db_readonly: ConnDB) -> None:
    # TODO: Enable this test on Windows when the read-only mode is implemented.
    if sys.platform == "win32":
        pytest.skip("Read-only mode has not been implemented on Windows yet")
    _, db = conn_db_readonly
    path = db.database_path
    read_only_db = kuzu.Database(path, read_only=True)
    conn = kuzu.Connection(read_only_db)
    with pytest.raises(RuntimeError, match="Cannot execute write operations in a read-only database!"):
        conn.execute("CREATE NODE TABLE test (id INT64, PRIMARY KEY(id));")


def test_max_db_size_exception() -> None:
    with pytest.raises(
        RuntimeError, match="Buffer manager exception: The given max db size should be at least 4194304 bytes."
    ):
        kuzu.Database("test.db", max_db_size=1024)

    with pytest.raises(RuntimeError, match="Buffer manager exception: The given max db size should be a power of 2."):
        kuzu.Database("test.db", max_db_size=4194305)


def test_buffer_pool_size_exception() -> None:
    with pytest.raises(
        RuntimeError, match="Buffer manager exception: The given buffer pool size should be at least 4KB."
    ):
        kuzu.Database("test.db", buffer_pool_size=1024)


def test_query_exception(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    with pytest.raises(RuntimeError, match="Binder exception: Table nonexisting does not exist."):
        conn.execute("MATCH (a:nonexisting) RETURN a;")
