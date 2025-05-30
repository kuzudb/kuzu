from __future__ import annotations

import pytest
from type_aliases import ConnDB


def test_timeout(conn_db_readonly: ConnDB) -> None:
    conn, _ = conn_db_readonly
    conn.set_query_timeout(1000)
    with pytest.raises(RuntimeError, match=r"Interrupted."):
        conn.execute("UNWIND RANGE(1,100000) AS x UNWIND RANGE(1, 100000) AS y RETURN COUNT(x + y);")
