from __future__ import annotations

from type_aliases import ConnDB


def test_install_and_load_httpfs(establish_connection: ConnDB) -> None:
    conn, db = establish_connection
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD EXTENSION httpfs")
