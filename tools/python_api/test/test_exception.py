import pytest
import sys
sys.path.append('../build/')
import kuzu


def test_exception(establish_connection):
    conn, db = establish_connection

    with pytest.raises(RuntimeError, match="Parameter asd not found."):
        conn.execute("MATCH (a:person) WHERE a.registerTime = $1 RETURN COUNT(*);", [("asd", 1)])

    with pytest.raises(RuntimeError, match="Binder exception: Cannot find property dummy for a."):
        conn.execute("MATCH (a:person) RETURN a.dummy;")


def test_db_path_exception():
    path = '/:* /? " < > |'
    with pytest.raises(RuntimeError, match='filesystem error'):
        kuzu.Database(str(path))
