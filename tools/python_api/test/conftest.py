import os

import pytest

from tools.python_api import _graphflowdb as gdb


# Note conftest is the default file name for sharing fixture through multiple test files. Do not change file name.
@pytest.fixture
def load_tiny_snb(tmp_path):
    tiny_snb_path = str("dataset/tinysnb")
    output_path = str(tmp_path)
    if os.path.exists(output_path):
        os.removedirs(output_path)
    loader = gdb.loader()
    loader.load(tiny_snb_path, output_path)
    return output_path


@pytest.fixture
def establish_connection(load_tiny_snb):
    db = gdb.database(load_tiny_snb)
    conn = gdb.connection(db)
    return conn, db
