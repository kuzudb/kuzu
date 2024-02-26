import pytest
import sys
import kuzu


def test_exception(establish_connection):
    conn, db = establish_connection

    with pytest.raises(RuntimeError, match="Parameter asd not found."):
        conn.execute("MATCH (a:person) WHERE a.registerTime = $1 RETURN COUNT(*);", {"asd": 1})

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

def test_read_only_exception(establish_connection):
    # TODO: Enable this test on Windows when the read-only mode is implemented.
    if sys.platform == "win32":
        pytest.skip("Read-only mode has not been implemented on Windows yet")
    _, db = establish_connection
    path = db.database_path
    read_only_db = kuzu.Database(path, read_only=True)
    conn = kuzu.Connection(read_only_db)
    with pytest.raises(RuntimeError, match="Cannot execute write operations in a read-only database!"):
        conn.execute("CREATE NODE TABLE test (id INT64, PRIMARY KEY(id));")

def test_max_db_size_exception():
    with pytest.raises(RuntimeError, match="Buffer manager exception: The given max db size should be at least 4194304 bytes."):
        kuzu.Database("test.db", max_db_size=1024)

    with pytest.raises(RuntimeError, match="Buffer manager exception: The given max db size should be a power of 2."):
        kuzu.Database("test.db", max_db_size=4194305)

def test_buffer_pool_size_exception():
    with pytest.raises(RuntimeError, match="Buffer manager exception: The given buffer pool size should be at least 4KB."):
        kuzu.Database("test.db", buffer_pool_size=1024)

def test_query_exception(establish_connection):
    conn, db = establish_connection
    with pytest.raises(RuntimeError, match="Binder exception: Table nonexisting does not exist."):
        conn.execute("MATCH (a:nonexisting) RETURN a;")
