from __future__ import annotations

from type_aliases import ConnDB


def test_get_column_names(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    with conn.execute("MATCH (a:person)-[e:knows]->(b:person) RETURN a.fName, e.date, b.ID;") as result:
        column_names = result.get_column_names()
        assert column_names[0] == "a.fName"
        assert column_names[1] == "e.date"
        assert column_names[2] == "b.ID"


def test_get_column_data_types(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    with conn.execute(
        "MATCH (p:person) RETURN p.ID, p.fName, p.isStudent, p.eyeSight, p.birthdate, p.registerTime, "
        "p.lastJobDuration, p.workedHours, p.courseScoresPerTerm;"
    ) as result:
        column_data_types = result.get_column_data_types()
        assert column_data_types[0] == "INT64"
        assert column_data_types[1] == "STRING"
        assert column_data_types[2] == "BOOL"
        assert column_data_types[3] == "DOUBLE"
        assert column_data_types[4] == "DATE"
        assert column_data_types[5] == "TIMESTAMP"
        assert column_data_types[6] == "INTERVAL"
        assert column_data_types[7] == "INT64[]"
        assert column_data_types[8] == "INT64[][]"


def test_get_schema(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    with conn.execute(
        "MATCH (p:person) RETURN p.ID, p.fName, p.isStudent, p.eyeSight, p.birthdate, p.registerTime, "
        "p.lastJobDuration, p.workedHours, p.courseScoresPerTerm;"
    ) as result:
        assert result.get_schema() == {
            "p.ID": "INT64",
            "p.fName": "STRING",
            "p.isStudent": "BOOL",
            "p.eyeSight": "DOUBLE",
            "p.birthdate": "DATE",
            "p.registerTime": "TIMESTAMP",
            "p.lastJobDuration": "INTERVAL",
            "p.workedHours": "INT64[]",
            "p.courseScoresPerTerm": "INT64[][]",
        }
