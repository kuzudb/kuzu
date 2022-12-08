import os
import sys
import pytest
sys.path.append('../build/')
import _kuzu as kuzu


# Note conftest is the default file name for sharing fixture through multiple test files. Do not change file name.
@pytest.fixture
def init_tiny_snb(tmp_path):
    if os.path.exists(tmp_path):
        os.rmdir(tmp_path)
    output_path = str(tmp_path)
    db = kuzu.database(output_path)
    conn = kuzu.connection(db)
    conn.execute("CREATE NODE TABLE person (ID INT64, fName STRING, gender INT64, isStudent BOOLEAN, isWorker BOOLEAN, "
                 "age INT64, eyeSight DOUBLE, birthdate DATE, registerTime TIMESTAMP, lastJobDuration "
                 "INTERVAL, workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][], PRIMARY "
                 "KEY (ID))")
    conn.execute("COPY person FROM \"../../../dataset/tinysnb/vPerson.csv\" (HEADER=true)")
    conn.execute(
        "create rel table knows (FROM person TO person, date DATE, meetTime TIMESTAMP, validInterval INTERVAL, "
        "comments STRING[], MANY_MANY);")
    conn.execute("COPY knows FROM \"../../../dataset/tinysnb/eKnows.csv\"")
    return output_path

@pytest.fixture
def establish_connection(init_tiny_snb):
    db = kuzu.database(init_tiny_snb, buffer_pool_size=256 * 1024 * 1024)
    conn = kuzu.connection(db, num_threads=4)
    return conn, db
