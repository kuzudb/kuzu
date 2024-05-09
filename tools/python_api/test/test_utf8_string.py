from __future__ import annotations

from tools.python_api.test.type_aliases import ConnDB
import pandas as pd
from test_helper import KUZU_ROOT

# required by python-lint


def test_utf8(conn_db_readwrite: ConnDB) -> None:
    conn, db = conn_db_readwrite
    conn.execute(
        "CREATE NODE TABLE Test("
        "pk STRING, "
        "PRIMARY KEY (pk))"
    )
    dtype_mapping = {
        "pk": "string[pyarrow]",
    }
    df = pd.read_csv(f"{KUZU_ROOT}/dataset/issue/duplicate-primary-key/data.csv", dtype=dtype_mapping, dtype_backend="pyarrow")
    response = conn.execute(f'COPY Test FROM (LOAD WITH HEADERS (pk STRING) FROM df WHERE NOT EXISTS {{MATCH (t:Test) WHERE t.pk = pk}} RETURN *)')
    while response.has_next():
        print(f"Inserted Test {response.get_next()}")

    response = conn.execute("LOAD WITH HEADERS (pk STRING) FROM df WHERE NOT EXISTS {MATCH (t:Test) WHERE t.pk = pk} RETURN *")
    assert not response.has_next()
