import os
import sys
import pytest
import shutil

sys.path.append('../build/')
import kuzu


def init_npy(conn):
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


def init_tensor(conn):
    conn.execute('create node table tensor (ID INT64, boolTensor BOOLEAN[], doubleTensor DOUBLE[][], '
                 'intTensor INT64[][][], oneDimInt INT64, PRIMARY KEY (ID));')
    conn.execute(
        'COPY tensor FROM "../../../dataset/tensor-list/vTensor.csv" (HEADER=true)')


def init_long_str(conn):
    conn.execute(
        "CREATE NODE TABLE personLongString (name STRING, spouse STRING, PRIMARY KEY(name))")
    conn.execute(
        'COPY personLongString FROM "../../../dataset/long-string-pk-tests/vPerson.csv"')
    conn.execute(
        "CREATE REL TABLE knowsLongString (FROM personLongString TO personLongString, MANY_MANY)")
    conn.execute(
        'COPY knowsLongString FROM "../../../dataset/long-string-pk-tests/eKnows.csv"')


def init_tinysnb(conn):
    tiny_snb_path = os.path.abspath(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../../../dataset/tinysnb")
    )
    schema_path = os.path.join(tiny_snb_path, "schema.cypher")
    with open(schema_path, "r") as f:
        for line in f.readlines():
            line = line.strip()
            if line:
                conn.execute(line)
    copy_path = os.path.join(tiny_snb_path, "copy.cypher")
    with open(copy_path, "r") as f:
        for line in f.readlines():
            line = line.strip()
            line = line.replace("dataset/tinysnb", "../../../dataset/tinysnb")
            if line:
                conn.execute(line)


def init_movie_serial(conn):
    conn.execute(
        "create node table moviesSerial (ID SERIAL, name STRING, length INT32, note STRING, PRIMARY KEY (ID));"
    )
    conn.execute(
        'copy moviesSerial from "../../../dataset/tinysnb-serial/vMovies.csv"'
    )


@pytest.fixture
def init_db(tmp_path):
    if os.path.exists(tmp_path):
        shutil.rmtree(tmp_path)
    output_path = str(tmp_path)
    db = kuzu.Database(output_path)
    conn = kuzu.Connection(db)
    init_tinysnb(conn)
    init_npy(conn)
    init_tensor(conn)
    init_long_str(conn)
    init_movie_serial(conn)
    return output_path


@pytest.fixture
def establish_connection(init_db):
    db = kuzu.Database(init_db, buffer_pool_size=256 * 1024 * 1024)
    conn = kuzu.Connection(db, num_threads=4)
    return conn, db


@pytest.fixture
def get_tmp_path(tmp_path):
    return str(tmp_path)
