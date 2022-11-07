import os

import pytest

from tools.python_api import _graphflowdb as gdb


# Note conftest is the default file name for sharing fixture through multiple test files. Do not change file name.
@pytest.fixture
def init_tiny_snb(tmp_path):
    if os.path.exists(tmp_path):
        os.rmdir(tmp_path)
    output_path = str(tmp_path)
    db = gdb.database(output_path)
    conn = gdb.connection(db)
    conn.execute("CREATE NODE TABLE person (ID INT64, fName STRING, gender INT64, isStudent BOOLEAN, isWorker BOOLEAN, "
                 "age INT64, eyeSight DOUBLE, birthdate DATE, registerTime TIMESTAMP, lastJobDuration "
                 "INTERVAL, workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][], PRIMARY "
                 "KEY (ID))")
    conn.execute("COPY person FROM \"dataset/tinysnb/vPerson.csv\" (HEADER=true)")
    return output_path


@pytest.fixture
def establish_connection(init_tiny_snb):
    db = gdb.database(init_tiny_snb, buffer_pool_size = 256 * 1024 * 1024)
    conn = gdb.connection(db, num_threads=4)
    return conn, db
