from __future__ import annotations

from tools.python_api.test.type_aliases import ConnDB

# required by python-lint


def test_param_empty(conn_db_readwrite: ConnDB) -> None:
    conn, db = conn_db_readwrite
    lst = [[]]
    conn.execute("CREATE NODE TABLE tab(id SERIAL, lst INT64[][], PRIMARY KEY(id))")
    result = conn.execute("CREATE (t:tab {lst: $1}) RETURN t.*", {"1": lst})
    assert result.has_next()
    assert result.get_next() == [0, lst]
    assert not result.has_next()
    result.close()


def test_issue_2874(conn_db_readwrite: ConnDB) -> None:
    conn, db = conn_db_readwrite
    result = conn.execute("UNWIND $idList as tid MATCH (t:person {ID: tid}) RETURN t.fName;", {"idList": [1, 2, 3]})
    assert result.has_next()
    assert result.get_next() == ["Bob"]
    assert result.has_next()
    assert result.get_next() == ["Carol"]
    assert not result.has_next()
    result.close()


def test_issue_2906(conn_db_readwrite: ConnDB) -> None:
    conn, db = conn_db_readwrite
    result = conn.execute("MATCH (a:person) WHERE $1 > a.ID AND $1 < a.age / 5 RETURN a.fName;", {"1": 6})
    assert result.has_next()
    assert result.get_next() == ["Alice"]
    assert result.has_next()
    assert result.get_next() == ["Carol"]
    assert not result.has_next()
    result.close()


def test_issue_3135(conn_db_readwrite: ConnDB) -> None:
    conn, db = conn_db_readwrite
    conn.execute("CREATE NODE TABLE t1(id SERIAL, number INT32, PRIMARY KEY(id));")
    conn.execute("CREATE (:t1 {number: $1})", {"1": 2})
    result = conn.execute("MATCH (n:t1) RETURN n.number;")
    assert result.has_next()
    assert result.get_next() == [2]
    assert not result.has_next()
    result.close()


def test_empty_list2(conn_db_readwrite: ConnDB) -> None:
    conn, db = conn_db_readwrite
    conn.execute(
        """
        CREATE NODE TABLE SnapArtifactScan (
            artifact_name STRING,
            scan_columns STRING[],
            scan_filter STRING,
            scan_limit INT64,
            scan_id STRING,
            PRIMARY KEY(scan_id)
        )
        """
    )
    result = conn.execute(
        """
        MERGE (n:SnapArtifactScan { scan_id: $scan_id })
        SET n.artifact_name = $artifact_name, n.scan_columns = $scan_columns
        RETURN n.scan_id
    """,
        {
            "artifact_name": "taxi_zones",
            "scan_columns": [],
            "scan_id": "896de6b9c7b69fa2598def49e8c61de07949be374d229a82899c9c75994fad20",
        },
    )
    assert result.has_next()
    assert result.get_next() == ["896de6b9c7b69fa2598def49e8c61de07949be374d229a82899c9c75994fad20"]
    assert not result.has_next()
    result.close()


def test_empty_map(conn_db_readwrite: ConnDB) -> None:
    conn, db = conn_db_readwrite
    conn.execute(
        """
        CREATE NODE TABLE Test (
            id SERIAL,
            prop1 MAP(STRING, STRING),
            prop2 MAP(STRING, STRING[]),
            PRIMARY KEY(id)
        )
        """
    )
    result = conn.execute(
        """
            CREATE (n:Test {prop1: $prop1, prop2: $prop2})
            RETURN n.id, n.prop1, n.prop2
        """,
        {
            "prop1": {},
            "prop2": {"a": []},
        },
    )
    assert result.has_next()
    assert result.get_next() == [0, {}, {"a": []}]
    assert not result.has_next()
    result.close()


# TODO(Maxwell): check if we should change getCastCost() for the following test
# def test_issue_3248(conn_db_readwrite: ConnDB) -> None:
#     conn, db = conn_db_readwrite
#     # Define schema
#     conn.execute("CREATE NODE TABLE Item(id UINT64, item STRING, price DOUBLE, vector DOUBLE[2], PRIMARY KEY (id))")
#
#     # Add data
#     conn.execute("MERGE (a:Item {id: 1, item: 'apple', price: 2.0, vector: cast([3.1, 4.1], 'DOUBLE[2]')})")
#     conn.execute("MERGE (b:Item {id: 2, item: 'banana', price: 1.0, vector: cast([5.9, 26.5], 'DOUBLE[2]')})")
#
#     # Run similarity search
#     result = conn.execute("MATCH (a:Item) RETURN a.item, a.price,
#               array_cosine_similarity(a.vector, [6.0, 25.0]) AS sim ORDER BY sim DESC")
#     assert result.has_next()
#     assert result.get_next() == [2]
#     assert result.has_next()
#     assert result.get_next() == [2]
#     assert not result.has_next()
#     result.close()
