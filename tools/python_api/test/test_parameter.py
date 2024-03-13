from __future__ import annotations

import datetime

import pytest
from type_aliases import ConnDB


def test_bool_param(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    result = conn.execute(
        "MATCH (a:person) WHERE a.isStudent = $1 AND a.isWorker = $k RETURN COUNT(*)", {"1": False, "k": False}
    )
    assert result.has_next()
    assert result.get_next() == [1]
    assert not result.has_next()
    result.close()


def test_int_param(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    result = conn.execute("MATCH (a:person) WHERE a.age < $AGE RETURN COUNT(*)", {"AGE": 1})
    assert result.has_next()
    assert result.get_next() == [0]
    assert not result.has_next()
    result.close()


def test_double_param(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    result = conn.execute("MATCH (a:person) WHERE a.eyeSight = $E RETURN COUNT(*)", {"E": 5.0})
    assert result.has_next()
    assert result.get_next() == [2]
    assert not result.has_next()
    result.close()


def test_str_param(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN concat(a.fName, $S);", {"S": "HH"})
    assert result.has_next()
    assert result.get_next() == ["AliceHH"]
    assert not result.has_next()
    result.close()


def test_date_param(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    result = conn.execute("MATCH (a:person) WHERE a.birthdate = $1 RETURN COUNT(*);", {"1": datetime.date(1900, 1, 1)})
    assert result.has_next()
    assert result.get_next() == [2]
    assert not result.has_next()
    result.close()


def test_timestamp_param(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    result = conn.execute(
        "MATCH (a:person) WHERE a.registerTime = $1 RETURN COUNT(*);",
        {"1": datetime.datetime(2011, 8, 20, 11, 25, 30)},
    )
    assert result.has_next()
    assert result.get_next() == [1]
    assert not result.has_next()
    result.close()


def test_param_error1(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    with pytest.raises(RuntimeError, match="Parameter name must be of type string but got <class 'int'>"):
        conn.execute("MATCH (a:person) WHERE a.registerTime = $1 RETURN COUNT(*);", {1: 1})


def test_param_error2(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    with pytest.raises(RuntimeError, match="Parameters must be a dict"):
        conn.execute("MATCH (a:person) WHERE a.registerTime = $1 RETURN COUNT(*);", ["asd"])


def test_param_error3(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    with pytest.raises(RuntimeError, match="Parameters must be a dict"):
        conn.execute("MATCH (a:person) WHERE a.registerTime = $1 RETURN COUNT(*);", [("asd", 1, 1)])
