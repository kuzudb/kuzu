from __future__ import annotations

import kuzu
import pytest
from type_aliases import ConnDB


def test_exception(conn_db_readonly: ConnDB) -> None:
    conn, _ = conn_db_readonly

    with pytest.raises(RuntimeError, match=r"Parameter asd not found."):
        conn.execute("MATCH (a:person) WHERE a.registerTime = $1 RETURN COUNT(*);", {"asd": 1})

    with pytest.raises(RuntimeError, match=r"Binder exception: Cannot find property dummy for a."):
        conn.execute("MATCH (a:person) RETURN a.dummy;")


def test_db_path_exception() -> None:
    path = ":memory:"
    error_message = "Cannot open an in-memory database under READ ONLY mode."
    with pytest.raises(RuntimeError, match=error_message):
        kuzu.Database(path, read_only=True)


def test_read_only_exception(conn_db_readonly: ConnDB) -> None:
    _, db = conn_db_readonly
    path = db.database_path
    read_only_db = kuzu.Database(path, read_only=True)
    conn = kuzu.Connection(read_only_db)
    with pytest.raises(RuntimeError, match=r"Cannot execute write operations in a read-only database!"):
        conn.execute("CREATE NODE TABLE test (id INT64, PRIMARY KEY(id));")


def test_max_db_size_exception() -> None:
    with pytest.raises(
        RuntimeError, match=r"Buffer manager exception: The given max db size should be at least 8388608 bytes."
    ):
        kuzu.Database("test.db", max_db_size=1024)

    with pytest.raises(RuntimeError, match=r"Buffer manager exception: The given max db size should be a power of 2."):
        kuzu.Database("test.db", max_db_size=8388609)


def test_buffer_pool_size_exception() -> None:
    with pytest.raises(
        RuntimeError, match=r"Buffer manager exception: The given buffer pool size should be at least 4096 bytes."
    ):
        kuzu.Database("test.db", buffer_pool_size=1024)


def test_query_exception(conn_db_readonly: ConnDB) -> None:
    conn, _ = conn_db_readonly
    with pytest.raises(RuntimeError, match=r"Binder exception: Table nonexisting does not exist."):
        conn.execute("MATCH (a:nonexisting) RETURN a;")
