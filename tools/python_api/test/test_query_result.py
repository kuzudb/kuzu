from __future__ import annotations

from type_aliases import ConnDB


def test_get_execution_time(establish_connection: ConnDB) -> None:
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a")
    assert result.get_execution_time() > 0
    result.close()


def test_get_compiling_time(establish_connection: ConnDB) -> None:
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a")
    assert result.get_compiling_time() > 0
    result.close()


def test_get_num_tuples(establish_connection: ConnDB) -> None:
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a")
    assert result.get_num_tuples() == 1
    result.close()


def test_explain(establish_connection: ConnDB) -> None:
    conn, db = establish_connection
    result = conn.execute("EXPLAIN MATCH (a:person) WHERE a.ID = 0 RETURN a")
    assert result.get_num_tuples() == 1
    result.close()


def test_context_manager(establish_connection: ConnDB) -> None:
    conn, db = establish_connection
    with conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a") as result:
        assert result.get_num_tuples() == 1
        assert result.get_compiling_time() > 0

    # context exit guarantees immediately 'close' of the underlying QueryResult
    # (don't have to wait for __del__, which may not ever actually get called)
    assert result.is_closed
