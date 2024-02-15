def test_install_and_load_httpfs(establish_connection):
    conn, db = establish_connection
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD EXTENSION httpfs")
