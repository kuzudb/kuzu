from __future__ import annotations

import re

import pytest
from type_aliases import ConnDB


def test_bytes_param(conn_db_empty: ConnDB) -> None:
    conn, _ = conn_db_empty
    conn.execute("CREATE NODE TABLE tab(id SERIAL PRIMARY KEY, data BLOB)")

    data_0 = b"\x00\x01\x02\x03\xff"
    data_1 = b"testing"
    data_2 = b"A" * 4096  # max size: 4KB, see https://docs.kuzudb.com/cypher/data-types/#blob
    data_3 = b""  # empty
    data_4 = None  # null
    data_5 = "Hello 🌍"  # str

    cases = [
        data_0,
        data_1,
        data_2,
        data_3,
        data_4,
        data_5.encode("utf-8"),  # to bytes
    ]
    for data in cases:
        conn.execute("CREATE (t:tab {data: $data})", {"data": data})

    result = conn.execute("MATCH (t:tab) RETURN t.data ORDER BY t.id")
    assert result.get_next() == [data_0]
    assert result.get_next() == [data_1]
    assert result.get_next() == [data_2]
    assert result.get_next() == [data_3]
    assert result.get_next() == [data_4]
    assert result.get_next()[0].decode("utf-8") == data_5
    result.close()


def test_bytes_param_backwards_compatibility(conn_db_empty: ConnDB) -> None:
    conn, _ = conn_db_empty
    conn.execute("CREATE NODE TABLE tab(id SERIAL PRIMARY KEY, data BLOB)")

    binary = b"backwards_compatibility"
    string = "".join(f"\\x{b:02x}" for b in binary)  # to \xHH format

    assert isinstance(string, str)
    assert re.match(r"^(\\x[0-9a-f]{2})+$", string)

    conn.execute("CREATE (t:tab {data: BLOB($string)})", {"string": string})

    result = conn.execute("MATCH (t:tab) RETURN t.data")
    assert result.get_next() == [binary]
    result.close()


def test_bytes_param_where_clause(conn_db_empty: ConnDB) -> None:
    conn, _ = conn_db_empty
    conn.execute("CREATE NODE TABLE tab(id INT64 PRIMARY KEY, data BLOB)")

    data = b"where_clause"

    conn.execute("CREATE (t:tab {id: 0, data: $data})", {"data": b"some data"})
    conn.execute("CREATE (t:tab {id: 1, data: $data})", {"data": data})
    conn.execute("CREATE (t:tab {id: 2, data: $data})", {"data": b"some other data"})

    result = conn.execute("MATCH (t:tab) WHERE t.data = $search RETURN t.id", {"search": data})
    assert result.get_next() == [1]
    assert not result.has_next()
    result.close()


def test_bytes_param_update(conn_db_empty: ConnDB) -> None:
    conn, _ = conn_db_empty
    conn.execute("CREATE NODE TABLE tab(id SERIAL PRIMARY KEY, data BLOB)")

    initial = b"initial"
    updated = b"updated"

    conn.execute("CREATE (t:tab {data: $data})", {"data": initial})
    conn.execute("MATCH (t:tab) SET t.data = $new_data", {"new_data": updated})

    result = conn.execute("MATCH (t:tab) RETURN t.data")
    assert result.get_next() == [updated]
    result.close()


def test_bytes_param_mixed_types(conn_db_empty: ConnDB) -> None:
    conn, _ = conn_db_empty
    conn.execute("CREATE NODE TABLE tab(id SERIAL PRIMARY KEY, data BLOB, name STRING, value DOUBLE)")

    data = b"data"
    name = "name"
    value = 3.14

    params = {"data": data, "name": name, "value": value}
    conn.execute("CREATE (t:tab {data: $data, name: $name, value: $value})", params)

    result = conn.execute("MATCH (t:tab) RETURN t.data, t.name, t.value")
    assert result.get_next() == [data, name, value]
    result.close()


def test_bytes_param_relationship(conn_db_empty: ConnDB) -> None:
    conn, _ = conn_db_empty
    conn.execute("CREATE NODE TABLE person(name STRING PRIMARY KEY)")
    conn.execute("CREATE REL TABLE rel(FROM person TO person, data BLOB)")

    conn.execute("CREATE (p:person {name: 'Alice'})")
    conn.execute("CREATE (p:person {name: 'Bob'})")

    data = b"relationship"

    conn.execute(
        """
            MATCH (p1:person {name: 'Alice'}), (p2:person {name: 'Bob'})
            CREATE (p1)-[r:rel {data: $data}]->(p2)
        """,
        {"data": data},
    )

    result = conn.execute("MATCH ()-[r:rel]->() RETURN r.data")
    assert result.get_next() == [data]
    result.close()


def test_bytes_param_udf(conn_db_empty: ConnDB) -> None:
    conn, _ = conn_db_empty

    def reverse_bytes(data: bytes) -> bytes:
        return data[::-1]

    conn.create_function("reverse_bytes", reverse_bytes, ["BLOB"], "BLOB")

    data = b"hello"
    expected = b"olleh"

    result = conn.execute("RETURN reverse_bytes($data)", {"data": data})
    assert result.get_next() == [expected]

    result = conn.execute("RETURN reverse_bytes(NULL)")
    assert result.get_next() == [None]

    result.close()
