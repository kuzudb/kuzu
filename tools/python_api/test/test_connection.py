from __future__ import annotations

from typing import TYPE_CHECKING

import kuzu
import pytest

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
