from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

import pytest
from type_aliases import ConnDB

# required by python-lint
if TYPE_CHECKING:
    from pathlib import Path
import kuzu


def test_struct_param_access(conn_db_readwrite: ConnDB) -> None:
    conn, _ = conn_db_readwrite
    batch = [
        {
            "id": "1",
            "name": "Christopher",
            "age": "61",
            "net_worth": "21561030.8",
            "email": "wilcox@hotmail.com",
            "address": "588 Campbell Well Suite 335, Lawrencemouth, VI 07241",
            "phone": "358-202-1821x630",
            "comments": "Stage modern card send point future serve. Hot month might pass dinner chance.\nPerformance become own spring from analysis commercial. Draw effort structure many drug first.",
        },
        {
            "id": "2",
            "name": "Amanda",
            "age": "42",
            "net_worth": "31344480.2",
            "email": "cook@hotmail.com",
            "address": "6293 Bright Centers, Chenton, MO 36829",
            "phone": "001-972-387-3913x966",
            "comments": "None agent some if skill. Often onto dog wish. Listen live hair garden contain worry time. Economic or institution statement energy take sit.",
        },
    ]
    conn.execute(
        """
        UNWIND $batch AS p
        RETURN p.name,
            p.age,
            p.net_worth,
            p.email,
            p.address,
            p.phone,
            p.comments
        """,
        parameters={"batch": batch},
    )


def test_array_binding(conn_db_readwrite: ConnDB) -> None:
    conn, _ = conn_db_readwrite
    conn.execute("CREATE NODE TABLE node(id STRING, embedding DOUBLE[3], PRIMARY KEY(id))")
    conn.execute("CREATE (d:node {id: 'test', embedding: $emb})", {"emb": [3, 5, 2]})
    result = conn.execute(
        """
        MATCH (d:node)
        RETURN d.id, array_cosine_similarity(d.embedding, $emb)
        """,
        {"emb": [4.3, 5.2, 6.7]},
    )
    assert result.get_next() == ["test", pytest.approx(0.8922316795174099)]
    # TODO(maxwell): fixme. The following case should be executed successfully.
    # with pytest.raises(RuntimeError) as err:
    #     conn.execute(
    #         """
    #         MATCH (d:node)
    #         RETURN d.id, array_cosine_similarity($emb1, $emb)
    #         """, {"emb": [4.3, 5.2, 6.7], "emb1": [2.2, 3.3, 5.5]}
    #     )
    # assert str(err.value) == "Binder exception: Left and right type are both ANY, which is not currently supported."


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


def test_int64_list_param(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    result = conn.execute("MATCH (a:person {workedHours: $1}) RETURN COUNT(*);", {"1": [3, 4, 5, 6, 7]})
    assert result.has_next()
    assert result.get_next() == [1]
    assert not result.has_next()
    result.close()


def test_int64_list_list_param(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    result = conn.execute(
        "MATCH (a:person) WHERE a.courseScoresPerTerm = $1 OR a.courseScoresPerTerm = $2 RETURN COUNT(*);",
        {"1": [[8, 10]], "2": [[7, 4], [8, 8], [9]]},
    )
    assert result.has_next()
    assert result.get_next() == [2]
    assert not result.has_next()
    result.close()


def test_string_list_param(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    result = conn.execute("MATCH (a:person {usedNames: $1}) RETURN COUNT(*);", {"1": ["Carmen", "Fred"]})
    assert result.has_next()
    assert result.get_next() == [1]
    assert not result.has_next()
    result.close()


def test_map_param(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    conn.execute(
        "CREATE NODE TABLE tab(id int64, mp MAP(double, int64), mp2 MAP(int64, double), mp3 MAP(string, string), mp4 MAP(string, string)[], primary key(id))"
    )
    result = conn.execute(
        "MERGE (t:tab {id: 0, mp: $1, mp2: $2, mp3: $3, mp4: $4}) RETURN t.*",
        {
            "1": {"key": [1.0, 2, 2.2], "value": [5, 3, -1]},
            "2": {"key": [5, 4, 0], "value": [-0.5, 0, 2.2]},
            "3": {"key": ["a", "b", "c"], "value": [1, "2", "3"]},
            "4": [{"key": ["a"], "value": ["b"]}],
        },
    )
    assert result.has_next()
    assert result.get_next() == [
        0,
        {1.0: 5, 2.0: 3, 2.2: -1},
        {5: -0.5, 4: -0.0, 0: 2.2},
        {"a": "1", "b": "2", "c": "3"},
        [{"a": "b"}],
    ]
    assert not result.has_next()
    result.close()


def test_general_list_param(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    conn.execute(
        "CREATE NODE TABLE tab(id int64, lst1 BOOL[], lst2 DOUBLE[], lst3 TIMESTAMP[], lst4 DATE[], lst5 INTERVAL[], lst6 STRING[], PRIMARY KEY(id))"
    )
    lst1 = [True, False]
    lst2 = [1.0, 2.0]
    lst3 = [datetime.datetime(2019, 11, 12, 11, 25, 30), datetime.datetime(1987, 2, 15, 3, 0, 2)]
    lst4 = [datetime.date(2019, 11, 12), datetime.date(1987, 2, 15)]
    lst5 = [lst3[0] - lst3[1]]
    lst6 = [1, "2", "3"]
    lst6ToString = ["1", "2", "3"]
    result = conn.execute(
        "MERGE (t:tab {id: 0, lst1: $1, lst2: $2, lst3: $3, lst4: $4, lst5: $5, lst6: $6}) RETURN t.*",
        {"1": lst1, "2": lst2, "3": lst3, "4": lst4, "5": lst5, "6": lst6},
    )
    assert result.has_next()
    assert result.get_next() == [0, lst1, lst2, lst3, lst4, lst5, lst6ToString]
    assert not result.has_next()
    result.close()


def test_null_resolution(tmp_path: Path) -> None:
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    conn.execute(
        "CREATE NODE TABLE tab(id SERIAL, lst1 INT64[], mp1 MAP(STRING, STRING), "
        "nest MAP(STRING, MAP(STRING, INT64))[], PRIMARY KEY(id))"
    )
    lst1 = [1, 2, 3, None]
    mp1 = {"key": ["a", "b", "c", "o"], "value": ["x", "y", "z", None]}
    nest = [
        {"key": ["2"], "value": [{"key": ["foo", "bar"], "value": [1, 2]}]},
        {"key": ["1"], "value": [{"key": [], "value": []}]},
    ]
    result = conn.execute("MERGE (t:tab {lst1: $1, mp1: $2, nest: $3}) RETURN t.*", {"1": lst1, "2": mp1, "3": nest})
    assert result.has_next()
    assert result.get_next() == [
        0,
        lst1,
        {"a": "x", "b": "y", "c": "z", "o": None},
        [{"2": {"foo": 1, "bar": 2}}, {"1": {}}],
    ]
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


def test_param(conn_db_readwrite: ConnDB) -> None:
    conn, db = conn_db_readwrite
    conn.execute("CREATE NODE TABLE NodeOne(id INT64, name STRING, PRIMARY KEY(id));")
    conn.execute("CREATE NODE TABLE NodeTwo(id INT64, name STRING, PRIMARY KEY(id));")
    conn.execute("CREATE Rel TABLE RelA(from NodeOne to NodeOne);")
    conn.execute("CREATE Rel TABLE RelB(from NodeTwo to NodeOne, id int64, name String);")
    conn.execute('CREATE (t: NodeOne {id:1, name: "Alice"});')
    conn.execute('CREATE (t: NodeOne {id:2, name: "Jack"});')
    conn.execute('CREATE (t: NodeTwo {id:3, name: "Bob"});')
    result = conn.execute(
        "MATCH (a:NodeOne { id: $a_id }),"
        "(b:NodeTwo { id: $b_id }),"
        "(c: NodeOne{ id: $c_id } )"
        " MERGE"
        " (a)-[:RelA]->(c),"
        " (b)-[r:RelB { id: 2, name: $my_param }]->(c)"
        " return r.*;",
        {"a_id": 1, "b_id": 3, "c_id": 2, "my_param": None},
    )
    assert result.has_next()
    assert result.get_next() == [2, None]
    result.close()


def test_param_error4(conn_db_readonly: ConnDB) -> None:
    conn, db = conn_db_readonly
    with pytest.raises(
        RuntimeError,
        match="Runtime exception: Cannot convert Python object to Kuzu value : INT8  is incompatible with TIMESTAMP",
    ):
        conn.execute(
            "MATCH (a:person {workedHours: $1}) RETURN COUNT(*);", {"1": [1, 2, datetime.datetime(2023, 3, 25)]}
        )


def test_dict_conversion(conn_db_readwrite: ConnDB) -> None:
    conn, db = conn_db_readwrite
    # Interpret as MAP.
    result = conn.execute("RETURN $st", {"st": {"key": [1, 2, 3], "value": [3, 7, 98]}})
    assert result.get_next() == [{1: 3, 2: 7, 3: 98}]
    # Interpret as STRUCT since the first field name is not "key".
    result = conn.execute("RETURN $st", {"st": {"key1": [1, 2, 3], "value": [3, 7, 98]}})
    assert result.get_next() == [{"key1": [1, 2, 3], "value": [3, 7, 98]}]
    # Interpret as STRUCT since the number of elements in key and value doesn't match.
    result = conn.execute("RETURN $st", {"st": {"key": [1, 2], "value": [3, 7, 98, 4]}})
    assert result.get_next() == [{"key": [1, 2], "value": [3, 7, 98, 4]}]
