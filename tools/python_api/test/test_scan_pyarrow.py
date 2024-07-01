from datetime import datetime

import pyarrow as pa
import pytest
from type_aliases import ConnDB

def test_pyarrow_basic(conn_db_readonly : ConnDB) -> None:
    conn, db = conn_db_readonly
    tab = pa.Table.from_arrays(
        [
            [1, 2, 3],
            ['a', 'b', 'c'],
            [True, False, None],
        ],
        names=["col1", "col2", "col3"]
    )
    result = conn.execute("LOAD FROM tab RETURN *")
    assert (result.get_next() == [1, 'a', True])
    assert (result.get_next() == [2, 'b', False])
    assert (result.get_next() == [3, 'c', None])
