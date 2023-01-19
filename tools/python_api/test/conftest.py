import os
import sys
import pytest
import shutil

sys.path.append('../build/')
import kuzu


# Note conftest is the default file name for sharing fixture through multiple test files. Do not change file name.
@pytest.fixture
def init_tiny_snb(tmp_path):
    if os.path.exists(tmp_path):
        shutil.rmtree(tmp_path)
    output_path = str(tmp_path)
    db = kuzu.Database(output_path)
    conn = kuzu.Connection(db)
    conn.execute("CREATE NODE TABLE person (ID INT64, fName STRING, gender INT64, isStudent BOOLEAN, isWorker BOOLEAN, "
                 "age INT64, eyeSight DOUBLE, birthdate DATE, registerTime TIMESTAMP, lastJobDuration "
                 "INTERVAL, workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][], PRIMARY "
                 "KEY (ID))")
    conn.execute(
        "COPY person FROM \"../../../dataset/tinysnb/vPerson.csv\" (HEADER=true)")
    conn.execute(
        "create rel table knows (FROM person TO person, date DATE, meetTime TIMESTAMP, validInterval INTERVAL, "
        "comments STRING[], MANY_MANY);")
    conn.execute("COPY knows FROM \"../../../dataset/tinysnb/eKnows.csv\"")
    conn.execute("create node table organisation (ID INT64, name STRING, orgCode INT64, mark DOUBLE, score INT64, history STRING, licenseValidInterval INTERVAL, rating DOUBLE, PRIMARY KEY (ID))")
    conn.execute(
        'COPY organisation FROM "../../../dataset/tinysnb/vOrganisation.csv" (HEADER=true)')
    conn.execute(
        'create rel table workAt (FROM person TO organisation, year INT64, MANY_ONE)')
    conn.execute(
        'COPY workAt FROM "../../../dataset/tinysnb/eWorkAt.csv" (HEADER=true)')
    return output_path


@pytest.fixture
def establish_connection(init_tiny_snb):
    db = kuzu.Database(init_tiny_snb, buffer_pool_size=256 * 1024 * 1024)
    conn = kuzu.Connection(db, num_threads=4)
    return conn, db


@pytest.fixture
def get_tmp_path(tmp_path):
    return str(tmp_path)
