import subprocess
import sys
import time
from pathlib import Path
from textwrap import dedent

import kuzu


def run_query_in_new_process(tmp_path: Path, build_dir: Path):
    code = dedent(
        f"""
        import sys
        sys.path.append(r"{build_dir!s}")

        import kuzu
        db = kuzu.Database(r"{tmp_path!s}")
        print(r"{tmp_path!s}")

        conn = kuzu.Connection(db)
        conn.execute("CREATE NODE TABLE tab (id INT64, PRIMARY KEY (id));")
        conn.execute("UNWIND RANGE(1,100000) AS x UNWIND RANGE(1, 100000) AS y CREATE (:tab {{id: x * 100000 + y}});")
        """
    )
    return subprocess.Popen([sys.executable, "-c", code])


# Kill the database while it's in the middle of executing a long persistent query
# When we reload the database it should
def test_replay_after_kill(tmp_path: Path, build_dir: Path) -> None:
    proc = run_query_in_new_process(tmp_path, build_dir)
    time.sleep(5)
    proc.kill()
    proc.wait(5)
    with kuzu.Database(tmp_path) as db:
        pass
