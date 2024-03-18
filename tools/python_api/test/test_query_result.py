from __future__ import annotations

from type_aliases import ConnDB


def test_get_execution_time(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a")
    assert result.get_execution_time() > 0
    result.close()


def test_get_compiling_time(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a")
    assert result.get_compiling_time() > 0
    result.close()


def test_get_num_tuples(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a")
    assert result.get_num_tuples() == 1
    result.close()


def test_explain(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    result = conn.execute("EXPLAIN MATCH (a:person) WHERE a.ID = 0 RETURN a")
    assert result.get_num_tuples() == 1
    result.close()


def test_context_manager(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    with conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a") as result:
        assert result.get_num_tuples() == 1
        assert result.get_compiling_time() > 0

    # context exit guarantees immediately 'close' of the underlying QueryResult
    # (don't have to wait for __del__, which may not ever actually get called)
    assert result.is_closed
