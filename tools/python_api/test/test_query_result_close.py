import subprocess
import sys


def test_query_result_close(get_tmp_path):
    code = [
        'import sys',
        'sys.path.append("../build/")',
        'import kuzu',
        'db = kuzu.Database("' + get_tmp_path + '")',
        'conn = kuzu.Connection(db)',
        'conn.execute(\'CREATE NODE TABLE person (ID INT64, fName STRING, gender INT64,\
            isStudent BOOLEAN, isWorker BOOLEAN, age INT64, eyeSight DOUBLE,\
            birthdate DATE, registerTime TIMESTAMP, lastJobDuration INTERVAL,\
            workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][],\
            PRIMARY KEY (ID))\')',
        'conn.execute(\'COPY person FROM \"../../../dataset/tinysnb/vPerson.csv\" (HEADER=true)\')',
        'result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.isStudent;")',
        # 'result.close()',
    ]
    code = ';'.join(code)
    result = subprocess.run([sys.executable, '-c', code])
    assert result.returncode == 0
