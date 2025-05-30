from __future__ import annotations

import kuzu


def test_iteration_list() -> None:
    db = kuzu.Database(database_path=":memory:")
    assert not db.is_closed
    assert db._database is not None

    conn = kuzu.Connection(db)
    conn.execute("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
    conn.execute("CREATE (:person {name: 'Alice', age: 30});")
    conn.execute("CREATE (:person {name: 'Bob', age: 40});")
    result = conn.execute("MATCH (p:person) RETURN p.*")
    assert result.get_num_tuples() == 2

    rows = list(result)
    assert rows[0] == ["Alice", 30]
    assert rows[1] == ["Bob", 40]


def test_iteration_loop() -> None:
    db = kuzu.Database(database_path=":memory:")
    assert not db.is_closed
    assert db._database is not None

    conn = kuzu.Connection(db)
    conn.execute("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
    conn.execute("CREATE (:person {name: 'Alice', age: 30});")
    conn.execute("CREATE (:person {name: 'Bob', age: 40});")
    result = conn.execute("MATCH (p:person) RETURN p.*")
    assert result.get_num_tuples() == 2

    rows = []
    for row in result:
        rows.append(row)

    assert rows[0] == ["Alice", 30]
    assert rows[1] == ["Bob", 40]


def test_get_all() -> None:
    db = kuzu.Database(database_path=":memory:")
    assert not db.is_closed
    assert db._database is not None

    conn = kuzu.Connection(db)
    conn.execute("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
    conn.execute("CREATE (:person {name: 'Alice', age: 30});")
    conn.execute("CREATE (:person {name: 'Bob', age: 40});")
    result = conn.execute("MATCH (p:person) RETURN p.*")
    assert result.get_num_tuples() == 2

    rows = result.get_all()

    assert rows[0] == ["Alice", 30]
    assert rows[1] == ["Bob", 40]


def test_get_n() -> None:
    db = kuzu.Database(database_path=":memory:")
    assert not db.is_closed
    assert db._database is not None

    conn = kuzu.Connection(db)
    conn.execute("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
    conn.execute("CREATE (:person {name: 'Alice', age: 30});")
    conn.execute("CREATE (:person {name: 'Bob', age: 40});")
    conn.execute("CREATE (:person {name: 'Cole', age: 20});")

    # Basic
    result = conn.execute("MATCH (p:person) RETURN p.*")
    assert result.get_num_tuples() == 3

    rows = result.get_n(2)

    assert len(rows) == 2
    assert rows[0] == ["Alice", 30]
    assert rows[1] == ["Bob", 40]

    rows = result.get_n(1)

    assert len(rows) == 1
    assert rows[0] == ["Cole", 20]

    rows = result.get_n(3)

    assert len(rows) == 0

    # Empty rows on excess count
    result = conn.execute("MATCH (p:person) RETURN p.*")
    assert result.get_num_tuples() == 3

    rows = result.get_n(10)

    assert len(rows) == 3
    assert rows[0] == ["Alice", 30]
    assert rows[1] == ["Bob", 40]
    assert rows[2] == ["Cole", 20]
