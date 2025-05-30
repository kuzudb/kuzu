import pyarrow as pa
import pytest
from type_aliases import ConnDB


def test_pyarrow_basic(conn_db_readonly: ConnDB) -> None:
    conn, _ = conn_db_readonly
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


def test_pyarrow_copy_from_parameterized_df(conn_db_readwrite: ConnDB) -> None:
    conn, _ = conn_db_readwrite

    def get_tab_func():
        return pa.Table.from_arrays(
            [
                pa.array([1, 2, 3], type=pa.int32()),
                pa.array(["a", "b", "c"], type=pa.string()),
                pa.array([True, False, None], type=pa.bool_()),
            ],
            names=["id", "A", "B"],
        )

    conn.execute("CREATE NODE TABLE pyarrowtab(id INT32, A STRING, B BOOL, PRIMARY KEY(id))")
    conn.execute("COPY pyarrowtab FROM $tab", {"tab": get_tab_func()})
    result = conn.execute("MATCH (t:pyarrowtab) RETURN t.id AS id, t.A AS A, t.B AS B ORDER BY t.id")
    assert result.get_next() == [1, "a", True]
    assert result.get_next() == [2, "b", False]
    assert result.get_next() == [3, "c", None]

    rels = pa.Table.from_arrays(
        [pa.array([1, 2, 3], type=pa.int32()), pa.array([2, 3, 1], type=pa.int32())], names=["from", "to"]
    )
    conn.execute("CREATE REL TABLE pyarrowrel(FROM pyarrowtab TO pyarrowtab)")
    conn.execute("COPY pyarrowrel FROM $tab", {"tab": rels})
    result = conn.execute("MATCH (a:pyarrowtab)-[:pyarrowrel]->(b:pyarrowtab) RETURN a.id, b.id ORDER BY a.id")
    assert result.get_next() == [1, 2]
    assert result.get_next() == [2, 3]
    assert result.get_next() == [3, 1]


def test_pyarrow_copy_from_invalid_source(conn_db_readwrite: ConnDB) -> None:
    conn, _ = conn_db_readwrite
    conn.execute("CREATE NODE TABLE pyarrowtab(id INT32, A STRING, B BOOL, PRIMARY KEY(id))")
    with pytest.raises(
        RuntimeError,
        match=r"Binder exception: Trying to scan from unsupported data type INT8\[\]. The only parameter types that can be scanned from are pandas/polars dataframes and pyarrow tables.",
    ):
        conn.execute("COPY pyarrowtab FROM $tab", {"tab": [1, 2, 3]})


def test_pyarrow_copy_from(conn_db_readwrite: ConnDB) -> None:
    conn, _ = conn_db_readwrite
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
    conn, _ = conn_db_readwrite
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
    conn, _ = conn_db_readwrite
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


def test_copy_from_pyarrow_multi_pairs(conn_db_readwrite: ConnDB) -> None:
    conn, _ = conn_db_readwrite
    conn.execute("CREATE NODE TABLE prof(id INT64, PRIMARY KEY(id))")
    conn.execute("CREATE (p:prof {id: 3});")
    conn.execute("CREATE (p:prof {id: 4});")
    conn.execute("CREATE NODE TABLE student(id INT64, PRIMARY KEY(id))")
    conn.execute("CREATE (p:student {id: 2});")
    conn.execute("CREATE REL TABLE teaches(from prof to prof, from prof to student, length int64)")
    df = pa.Table.from_arrays(
        [
            pa.array([3], type=pa.int64()),
            pa.array([4], type=pa.int64()),
            pa.array([252], type=pa.int64()),
        ],
        names=["from", "to", "length"],
    )
    conn.execute("COPY teaches from df (from = 'prof', to = 'prof');")
    result = conn.execute("match (:prof)-[e:teaches]->(:prof) return e.*")
    assert result.has_next()
    assert result.get_next()[0] == 252
    assert not result.has_next()
