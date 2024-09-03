from __future__ import annotations

from type_aliases import ConnDB
import pathlib

def test_install_and_load_httpfs(conn_db_readonly: ConnDB) -> None:
    httpfs_path = (
        pathlib.Path(__file__).parent.parent.parent.parent / "extension" / "httpfs" /  "build" /  "libhttpfs.kuzu_extension"
    )
    httpfs_path = httpfs_path.resolve()
    httpfs_path_str = str(httpfs_path)
    httpfs_path_str = httpfs_path_str.replace("\\", "/")
    conn, _ = conn_db_readonly
    conn.execute("INSTALL httpfs")
    conn.execute('LOAD EXTENSION "{}"'.format(httpfs_path_str))
