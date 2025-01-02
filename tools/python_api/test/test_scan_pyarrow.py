import pyarrow as pa
import pytest
from type_aliases import ConnDB


def test_pyarrow_basic(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    tab = pa.Table.from_arrays(
        [
            [1, 2, 3],
            ["a", "b", "c"],
            [True, False, None],
        ],
        names=["col1", "col2", "col3"],
    )
    result = conn.execute("LOAD FROM tab RETURN * ORDER BY col1")
    assert result.get_next() == [1, "a", True]
    assert result.get_next() == [2, "b", False]
    assert result.get_next() == [3, "c", None]


def test_pyarrow_copy_from(conn_db_readwrite: ConnDB) -> None:
    conn, db = conn_db_readwrite
    tab = pa.Table.from_arrays(
        [
            pa.array([1, 2, 3], type=pa.int32()),
            pa.array(["a", "b", "c"], type=pa.string()),
            pa.array([True, False, None], type=pa.bool_()),
        ],
        names=["id", "A", "B"],
    )
    conn.execute("CREATE NODE TABLE pyarrowtab(id INT32, A STRING, B BOOL, PRIMARY KEY(id))")
    conn.execute("COPY pyarrowtab FROM tab")
    result = conn.execute("MATCH (t:pyarrowtab) RETURN t.id AS id, t.A AS A, t.B AS B ORDER BY t.id")
    assert result.get_next() == [1, "a", True]
    assert result.get_next() == [2, "b", False]
    assert result.get_next() == [3, "c", None]

    rels = pa.Table.from_arrays(
        [pa.array([1, 2, 3], type=pa.int32()), pa.array([2, 3, 1], type=pa.int32())], names=["from", "to"]
    )
    conn.execute("CREATE REL TABLE pyarrowrel(FROM pyarrowtab TO pyarrowtab)")
    conn.execute("COPY pyarrowrel FROM rels")
    result = conn.execute("MATCH (a:pyarrowtab)-[:pyarrowrel]->(b:pyarrowtab) RETURN a.id, b.id ORDER BY a.id")
    assert result.get_next() == [1, 2]
    assert result.get_next() == [2, 3]
    assert result.get_next() == [3, 1]

    conn.execute("DROP TABLE pyarrowrel")

    rels = pa.Table.from_arrays(
        [
            pa.array([1, 2, 3], type=pa.int32()),
            pa.array([2, 3, 1], type=pa.int32()),
            pa.array(["honk", "shoo", "mimimimimimimi"], type=pa.string()),
        ],
        names=["foo", "bar", "spongebobmeboy"],
    )

    conn.execute("CREATE REL TABLE pyarrowrel(FROM pyarrowtab TO pyarrowtab, a_distraction STRING)")
    conn.execute("COPY pyarrowrel FROM rels")
    result = conn.execute(
        "MATCH (a:pyarrowtab)-[r:pyarrowrel]->(b:pyarrowtab) RETURN a.id, r.a_distraction, b.id ORDER BY a.id"
    )
    assert result.get_next() == [1, "honk", 2]
    assert result.get_next() == [2, "shoo", 3]
    assert result.get_next() == [3, "mimimimimimimi", 1]


def test_pyarrow_scan_ignore_errors(conn_db_readwrite: ConnDB) -> None:
    conn, db = conn_db_readwrite
    tab = pa.Table.from_arrays(
        [
            pa.array([1, 2, 3, 1], type=pa.int32()),
        ],
        names=["id"],
    )
    conn.execute("CREATE NODE TABLE ids(id INT64, PRIMARY KEY(id))")
    conn.execute("COPY ids FROM tab(IGNORE_ERRORS=true)")

    people = conn.execute("MATCH (i:ids) RETURN i.id")
    assert people.get_next() == [1]
    assert people.get_next() == [2]
    assert people.get_next() == [3]
    assert not people.has_next()

    warnings = conn.execute("CALL show_warnings() RETURN *")
    assert warnings.get_next()[1].startswith("Found duplicated primary key value 1")
    assert not warnings.has_next()


def test_pyarrow_scan_invalid_option(conn_db_readwrite: ConnDB) -> None:
    conn, db = conn_db_readwrite
    tab = pa.Table.from_arrays(
        [
            pa.array([1, 2, 3], type=pa.int32()),
        ],
        names=["id"],
    )
    conn.execute("CREATE NODE TABLE ids(id INT64, PRIMARY KEY(id))")
    error_message = "INVALID_OPTION Option not recognized by pyArrow scanner."
    with pytest.raises(RuntimeError, match=error_message):
        conn.execute("COPY ids FROM tab(INVALID_OPTION=1)")
