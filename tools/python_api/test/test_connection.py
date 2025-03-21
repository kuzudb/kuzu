from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

import kuzu
import pytest
from type_aliases import ConnDB

if TYPE_CHECKING:
    from pathlib import Path


def test_connection_close(tmp_path: Path, build_dir: Path) -> None:
    db_path = tmp_path / "test_connection_close.kuzu"
    db = kuzu.Database(database_path=db_path, read_only=False)
    conn = kuzu.Connection(db)
    conn.close()
    assert conn.is_closed
    pytest.raises(RuntimeError, conn.execute, "RETURN 1")
    db.close()


def test_connection_close_context_manager(tmp_path: Path, build_dir: Path) -> None:
    db_path = tmp_path / "test_connection_close_context_manager.kuzu"
    with kuzu.Database(database_path=db_path, read_only=False) as db:
        with kuzu.Connection(db) as conn:
            pass
        assert conn.is_closed
        pytest.raises(RuntimeError, conn.execute, "RETURN 1")
    assert db.is_closed


def run_long_query(conn):
    query = "UNWIND RANGE(1,1000000) AS x UNWIND RANGE(1, 1000000) AS y RETURN COUNT(x + y);"
    with pytest.raises(RuntimeError) as excinfo:
        conn.execute(query)
    assert "Interrupted" in str(excinfo.value)


def test_connection_interrupt(conn_db_readwrite: ConnDB) -> None:
    conn, _ = conn_db_readwrite
    execute_thread = threading.Thread(target=run_long_query, args=(conn,))
    execute_thread.start()
    time.sleep(5)
    conn.interrupt()
    execute_thread.join(timeout=100)
    assert not execute_thread.is_alive()
