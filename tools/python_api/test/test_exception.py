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
    if sys.platform == "win32":
        error_message = 'Failed to create directory'
    else:
        error_message = 'filesystem error'
    with pytest.raises(RuntimeError, match=error_message):
        kuzu.Database(str(path))
