import pytest


def test_exception(establish_connection):
    conn, db = establish_connection

    with pytest.raises(RuntimeError, match="Parameter asd not found."):
        conn.execute("MATCH (a:person) WHERE a.registerTime = $1 RETURN COUNT(*);", [("asd", 1)])

    with pytest.raises(RuntimeError, match="Binder exception: Cannot find property dummy under node a"):
        conn.execute("MATCH (a:person) RETURN a.dummy;")

    with pytest.raises(RuntimeError,
                       match="Buffer manager exception: Resizing to a smaller Buffer Pool Size is unsupported."):
        db.resize_buffer_manager(1)
