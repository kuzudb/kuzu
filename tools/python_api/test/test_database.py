from __future__ import annotations

import subprocess
import sys
from textwrap import dedent
from typing import TYPE_CHECKING

import kuzu
import pytest

if TYPE_CHECKING:
    from pathlib import Path


def open_database_on_subprocess(tmp_path: Path, build_dir: Path) -> None:
    code = dedent(
        f"""
        import sys
        sys.path.append(r"{build_dir!s}")

        import kuzu
        db = kuzu.Database(r"{tmp_path!s}")
        print(r"{tmp_path!s}")
    """
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr)


def test_database_close(tmp_path: Path, build_dir: Path) -> None:
    db_path = tmp_path / "test_database_close.kuzu"
    db = kuzu.Database(database_path=db_path, read_only=False)
    assert not db.is_closed
    assert db._database is not None

    # Open the database on a subprocess. It should raise an exception.
    # TODO: Enable this test on Windows when the read-only mode is implemented.
    if sys.platform != "win32":
        with pytest.raises(
            RuntimeError,
            match=r"RuntimeError: IO exception: Could not set lock on file",
        ):
            open_database_on_subprocess(db_path, build_dir)

    db.close()
    assert db.is_closed
    assert db._database is None

    # Try to close the database again. It should not raise any exceptions.
    db.close()
    assert db.is_closed
    assert db._database is None

    # Open the database on a subprocess. It should not raise any exceptions.
    # TODO: Enable this test on Windows when the read-only mode is implemented.
    if sys.platform != "win32":
        open_database_on_subprocess(db_path, build_dir)


def test_database_context_manager(tmp_path: Path, build_dir: Path) -> None:
    db_path = tmp_path / "test_database_context_manager.kuzu"
    with kuzu.Database(database_path=db_path, read_only=False) as db:
        assert not db.is_closed
        assert db._database is not None

        # Open the database on a subprocess. It should raise an exception.
        # TODO: Enable this test on Windows when the read-only mode is implemented.
        if sys.platform != "win32":
            with pytest.raises(
                RuntimeError,
                match=r"RuntimeError: IO exception: Could not set lock on file",
            ):
                open_database_on_subprocess(db_path, build_dir)

    # Open the database on a subprocess. It should not raise any exceptions.
    # TODO: Enable this test on Windows when the read-only mode is implemented.
    if sys.platform != "win32":
        open_database_on_subprocess(db_path, build_dir)
