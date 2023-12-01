def test_timeout(establish_connection):
    conn, _ = establish_connection
    conn.set_query_timeout(1000)
    try:
        conn.execute("MATCH (a:person)-[:knows*1..28]->(b:person) RETURN COUNT(*);")
    except RuntimeError as err:
        assert str(err) == "Interrupted."
