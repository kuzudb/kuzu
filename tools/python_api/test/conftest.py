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
                 "INTERVAL, workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][],  grades INT64[4], "
                 "height float, PRIMARY KEY (ID))")
    conn.execute("COPY person FROM \"../../../dataset/tinysnb/vPerson.csv\" (HEADER=true)")
    conn.execute(
        "create rel table knows (FROM person TO person, date DATE, meetTime TIMESTAMP, validInterval INTERVAL, "
        "comments STRING[], MANY_MANY);")
    conn.execute("COPY knows FROM \"../../../dataset/tinysnb/eKnows.csv\"")
    conn.execute("create node table organisation (ID INT64, name STRING, orgCode INT64, mark DOUBLE, score INT64, "
                 "history STRING,licenseValidInterval INTERVAL, rating DOUBLE, state STRUCT(revenue INT16, location "
                 "STRING[], stock STRUCT(price INT64[], volume INT64)), PRIMARY KEY (ID));")
    conn.execute('COPY organisation FROM "../../../dataset/tinysnb/vOrganisation.csv"')
    conn.execute('CREATE NODE TABLE movies (name STRING, length INT32, note STRING, description STRUCT(rating DOUBLE, '
                 'views INT64, release TIMESTAMP, film DATE), content BYTEA, PRIMARY KEY (name))')
    conn.execute('COPY movies FROM "../../../dataset/tinysnb/vMovies.csv"')
    conn.execute('create rel table workAt (FROM person TO organisation, year INT64, grading DOUBLE[2], rating float,'
                 ' MANY_ONE)')
    conn.execute('COPY workAt FROM "../../../dataset/tinysnb/eWorkAt.csv"')
    conn.execute('create node table tensor (ID INT64, boolTensor BOOLEAN[], doubleTensor DOUBLE[][], '
                 'intTensor INT64[][][], oneDimInt INT64, PRIMARY KEY (ID));')
    conn.execute(
        'COPY tensor FROM "../../../dataset/tensor-list/vTensor.csv" (HEADER=true)')
    conn.execute(
        "CREATE NODE TABLE personLongString (name STRING, spouse STRING, PRIMARY KEY(name))")
    conn.execute(
        'COPY personLongString FROM "../../../dataset/long-string-pk-tests/vPerson.csv"')
    conn.execute(
        "CREATE REL TABLE knowsLongString (FROM personLongString TO personLongString, MANY_MANY)")
    conn.execute(
        'COPY knowsLongString FROM "../../../dataset/long-string-pk-tests/eKnows.csv"')
    conn.execute(
        'create node table npyoned (i64 INT64,i32 INT32,i16 INT16,f64 DOUBLE,f32 FLOAT, PRIMARY KEY(i64));'
    )
    conn.execute(
        'copy npyoned from ("../../../dataset/npy-1d/one_dim_int64.npy",  "../../../dataset/npy-1d/one_dim_int32.npy", '
        ' "../../../dataset/npy-1d/one_dim_int16.npy",  "../../../dataset/npy-1d/one_dim_double.npy", '
        '"../../../dataset/npy-1d/one_dim_float.npy") by column;'
    )
    conn.execute(
        'create node table npytwod (id INT64, i64 INT64[3], i32 INT32[3], i16 INT16[3], f64 DOUBLE[3], f32 FLOAT[3],'
        'PRIMARY KEY(id));'
    )
    conn.execute(
        'copy npytwod from ("../../../dataset/npy-2d/id_int64.npy", "../../../dataset/npy-2d/two_dim_int64.npy", '
        '"../../../dataset/npy-2d/two_dim_int32.npy",  "../../../dataset/npy-2d/two_dim_int16.npy", '
        ' "../../../dataset/npy-2d/two_dim_double.npy", "../../../dataset/npy-2d/two_dim_float.npy") by column;'
    )
    return output_path


@pytest.fixture
def establish_connection(init_tiny_snb):
    db = kuzu.Database(init_tiny_snb, buffer_pool_size=256 * 1024 * 1024)
    conn = kuzu.Connection(db, num_threads=4)
    return conn, db


@pytest.fixture
def get_tmp_path(tmp_path):
    return str(tmp_path)
