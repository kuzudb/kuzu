import subprocess
import sys
from pathlib import Path
from textwrap import dedent

from test_helper import KUZU_ROOT
from conftest import get_db_file_path


def test_query_result_close(tmp_path: Path, build_dir: Path) -> None:
    db_path = get_db_file_path(tmp_path)
    code = dedent(f"""
        import sys
        sys.path.append(r"{build_dir!s}")

        import kuzu
        db = kuzu.Database(r"{db_path!s}")
        conn = kuzu.Connection(db)
        conn.execute('''
          CREATE NODE TABLE person (
            ID INT64,
            fName STRING,
            gender INT64,
            isStudent BOOLEAN,
            isWorker BOOLEAN,
            age INT64,
            eyeSight DOUBLE,
            birthdate DATE,
            registerTime TIMESTAMP,
            lastJobDuration INTERVAL,
            workedHours INT64[],
            usedNames STRING[],
            courseScoresPerTerm INT64[][],
            grades INT64[4],
            height float,
            u UUID,
            PRIMARY KEY (ID))
        ''')
        conn.execute('COPY person FROM "{KUZU_ROOT}/dataset/tinysnb/vPerson.csv" (HEADER=true)')
        result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.isStudent;")
        # result.close()
    """)
    result = subprocess.run([sys.executable, "-c", code])
    assert result.returncode == 0
