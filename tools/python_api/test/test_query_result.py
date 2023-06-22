def test_get_execution_time(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a")
    assert result.get_execution_time() > 0
    result.close()


def test_get_compiling_time(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a")
    assert result.get_compiling_time() > 0
    result.close()


def test_get_num_tuples(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a")
    assert result.get_num_tuples() == 1
    result.close()
