import subprocess
import sys
import time
from pathlib import Path
from textwrap import dedent

import kuzu


def run_query_in_new_process(tmp_path: Path, build_dir: Path, queries: str):
    code = (
        dedent(
            f"""
        import sys
        sys.path.append(r"{build_dir!s}")

        import kuzu
        db = kuzu.Database(r"{tmp_path!s}")
        """
        )
        + queries
    )
    return subprocess.Popen([sys.executable, "-c", code])


def run_query_then_kill(tmp_path: Path, build_dir: Path, queries: str):
    proc = run_query_in_new_process(tmp_path, build_dir, queries)
    time.sleep(5)
    proc.kill()
    proc.wait(5)
    # Force remove the lock file. Safe since proc.wait() ensures the process has terminated.
    Path(f"{tmp_path!s}/.lock").unlink(missing_ok=True)


# Kill the database while it's in the middle of executing a long persistent query
# When we reload the database we will replay from the WAL (which will be incomplete)
def test_replay_after_kill(tmp_path: Path, build_dir: Path) -> None:
    queries = dedent("""
    conn = kuzu.Connection(db)
    conn.execute("CREATE NODE TABLE tab (id INT64, PRIMARY KEY (id));")
    conn.execute("UNWIND RANGE(1,100000) AS x UNWIND RANGE(1, 100000) AS y CREATE (:tab {id: x * 100000 + y});")
    """)
    run_query_then_kill(tmp_path, build_dir, queries)
    with kuzu.Database(tmp_path) as db, kuzu.Connection(db) as conn:
        # previously committed queries should be valid after replaying WAL
        result = conn.execute("CALL show_tables() RETURN *")
        assert result.has_next()
        assert result.get_next()[1] == "tab"
        assert not result.has_next()
        result.close()


def test_replay_with_exception(tmp_path: Path, build_dir: Path) -> None:
    queries = dedent("""
    conn = kuzu.Connection(db)
    conn.execute("CREATE NODE TABLE tab (id INT64, PRIMARY KEY (id));")
    # some of these queries will throw exceptions
    for i in range(10):
        try:
            conn.execute(f"CREATE (:tab {{id: {i // 2}}})")
            assert i % 2 == 0
        except:
            assert i % 2 == 1
    conn.execute("UNWIND RANGE(1,100000) AS x UNWIND RANGE(1, 100000) AS y CREATE (:tab {id: x * 100000 + y});")
    """)
    run_query_then_kill(tmp_path, build_dir, queries)
    with kuzu.Database(tmp_path) as db, kuzu.Connection(db) as conn:
        # previously committed queries should be valid after replaying WAL
        result = conn.execute("match (t:tab) where t.id <= 5 return t.id")
        assert result.get_num_tuples() == 5
        for i in range(5):
            assert result.get_next()[0] == i
        assert not result.has_next()
        result.close()
