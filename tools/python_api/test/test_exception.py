import pytest


def test_exception(establish_connection):
    conn, db = establish_connection

    with pytest.raises(RuntimeError, match="Parameter asd not found."):
        conn.execute("MATCH (a:person) WHERE a.registerTime = $1 RETURN COUNT(*);", [("asd", 1)])

    with pytest.raises(RuntimeError, match="Binder exception: Node a does not have property dummy."):
        conn.execute("MATCH (a:person) RETURN a.dummy;")

    with pytest.raises(RuntimeError, match="Runtime exception: Cannot abs `INTERVAL`"):
        conn.execute("MATCH (a:person) RETURN abs(a.unstrIntervalProp);")
