from __future__ import annotations

import pytest
from type_aliases import ConnDB


def test_timeout(establish_connection: ConnDB) -> None:
    conn, _ = establish_connection
    conn.set_query_timeout(1000)
    with pytest.raises(RuntimeError, match="Interrupted."):
        conn.execute("MATCH (a:person)-[:knows*1..28]->(b:person) RETURN COUNT(*);")
