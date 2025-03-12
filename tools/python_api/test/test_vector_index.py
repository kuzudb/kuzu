from __future__ import annotations

from test_helper import KUZU_ROOT
from type_aliases import ConnDB


def test_vector_index(conn_db_readwrite: ConnDB) -> None:
    conn, _ = conn_db_readwrite
    query = "CREATE NODE TABLE embeddings (id int64, vec FLOAT[8], PRIMARY KEY (id));"
    conn.execute(query)
    query = f"""
    COPY embeddings FROM '{KUZU_ROOT}/dataset/embeddings/embeddings-8-1k.csv' (deLim=',');
    """
    conn.execute(query)
    query = "CALL CREATE_HNSW_INDEX('embeddings', 'e_hnsw_index', 'vec', distFunc := 'l2');"
    conn.execute(query)
    vec = [0.1521, 0.3021, 0.5366, 0.2774, 0.5593, 0.5589, 0.1365, 0.8557]
    res = conn.execute("""
                CALL QUERY_HNSW_INDEX('embeddings', 'e_hnsw_index', $q, 3) RETURN nn.id AS id ORDER BY id;
                """, {'q': vec}).get_as_arrow()
    assert len(res) == 3
    assert res["id"].to_pylist() == [133, 333, 444]
