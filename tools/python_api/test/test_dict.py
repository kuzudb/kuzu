from __future__ import annotations

import kuzu

from type_aliases import ConnDB


def test_get_next(conn_db_in_mem: ConnDB) -> None:
    conn, _ = conn_db_in_mem
    conn.execute("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
    conn.execute("CREATE (:person {name: 'Alice', age: 30});")
    conn.execute("CREATE (:person {name: 'Bob', age: 40});")

    result = conn.execute("MATCH (p:person) RETURN p.*")
    assert result.get_num_tuples() == 2
    result.rows_as_dict()

    row = result.get_next()
    assert row == {"p.name": "Alice", "p.age": 30}
    row = result.get_next()
    assert row == {"p.name": "Bob", "p.age": 40}


def test_get_all(conn_db_in_mem: ConnDB) -> None:
    conn, _ = conn_db_in_mem
    conn.execute("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
    conn.execute("CREATE (:person {name: 'Alice', age: 30});")
    conn.execute("CREATE (:person {name: 'Bob', age: 40});")

    result = conn.execute("MATCH (p:person) RETURN p.*")
    assert result.get_num_tuples() == 2
    result.rows_as_dict()

    rows = result.get_all()
    assert rows[0] == {"p.name": "Alice", "p.age": 30}
    assert rows[1] == {"p.name": "Bob", "p.age": 40}


def test_get_n(conn_db_in_mem: ConnDB) -> None:
    conn, _ = conn_db_in_mem
    conn.execute("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
    conn.execute("CREATE (:person {name: 'Alice', age: 30});")
    conn.execute("CREATE (:person {name: 'Bob', age: 40});")
    conn.execute("CREATE (:person {name: 'Cole', age: 20});")

    result = conn.execute("MATCH (p:person) RETURN p.*")
    assert result.get_num_tuples() == 3
    result.rows_as_dict()

    rows = result.get_n(2)

    assert len(rows) == 2
    assert rows[0] == {"p.name": "Alice", "p.age": 30}
    assert rows[1] == {"p.name": "Bob", "p.age": 40}

    rows = result.get_n(1)

    assert len(rows) == 1
    assert rows[0] == {"p.name": "Cole", "p.age": 20}

    rows = result.get_n(1)

    assert len(rows) == 0


def test_dict_iteration(conn_db_in_mem: ConnDB) -> None:
    conn, _ = conn_db_in_mem
    conn.execute("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
    conn.execute("CREATE (:person {name: 'Alice', age: 30});")
    conn.execute("CREATE (:person {name: 'Bob', age: 40});")
    result = conn.execute("MATCH (p:person) RETURN p.*")
    assert result.get_num_tuples() == 2

    result.rows_as_dict()
    rows = list(result)
    assert rows[0] == {"p.name": "Alice", "p.age": 30}
    assert rows[1] == {"p.name": "Bob", "p.age": 40}


def test_dict_iteration_inline(conn_db_in_mem: ConnDB) -> None:
    conn, _ = conn_db_in_mem
    conn.execute("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
    conn.execute("CREATE (:person {name: 'Alice', age: 30});")
    conn.execute("CREATE (:person {name: 'Bob', age: 40});")
    result = conn.execute("MATCH (p:person) RETURN p.*")
    assert result.get_num_tuples() == 2

    rows = list(result.rows_as_dict())
    assert rows[0] == {"p.name": "Alice", "p.age": 30}
    assert rows[1] == {"p.name": "Bob", "p.age": 40}


def test_on_off(conn_db_in_mem: ConnDB) -> None:
    conn, _ = conn_db_in_mem
    conn.execute("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
    conn.execute("CREATE (:person {name: 'Alice', age: 30});")
    conn.execute("CREATE (:person {name: 'Bob', age: 40});")
    conn.execute("CREATE (:person {name: 'Cole', age: 20});")

    result = conn.execute("MATCH (p:person) RETURN p.*")
    assert result.get_num_tuples() == 3

    row = result.get_next()
    assert row == ["Alice", 30]

    result.rows_as_dict()
    row = result.get_next()
    assert row == {"p.name": "Bob", "p.age": 40}

    result.rows_as_dict(False)
    row = result.get_next()
    assert row == ["Cole", 20]
