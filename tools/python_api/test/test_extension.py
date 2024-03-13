from __future__ import annotations

from type_aliases import ConnDB


def test_install_and_load_httpfs(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD EXTENSION httpfs")
