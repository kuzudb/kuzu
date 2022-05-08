from tools.python_api import to_panda
import datetime


def test_to_panda(establish_connection):
    conn, db = establish_connection
    query = "MATCH (o:organisation) return * limit 3"
    pd = to_panda.to_panda(db, query)
    assert pd.loc[0].tolist()[0:3] == [1, 'ABFsUni', 325]
    assert pd.loc[1].tolist()[0:3] == [4, 'CsWork', 934]
    assert pd.loc[2].tolist()[0:3] == [6, 'DEsWork', 824]


def test_to_panda_with_node_name(establish_connection):
    conn, db = establish_connection
    query = "MATCH (a:person)-[e:knows]->(b:person)-[e1:knows]->(c:person) return a,b,e"
    pd = to_panda.to_panda(db, query, node_name=['a', 'b'])
    assert pd.loc[0].tolist()[0:3] == [2, 'Bob', 2]
    assert pd.loc[0].tolist()[26:29] == [0, 'Alice', 1]
    assert pd.loc[1].tolist()[0:3] == [3, 'Carol', 1]
    assert pd.loc[1].tolist()[26:29] == [0, 'Alice', 1]
    assert pd.loc[2].tolist()[0:3] == [5, 'Dan', 2]
    assert pd.loc[2].tolist()[26:29] == [0, 'Alice', 1]


def test_to_panda_with_relation_name(establish_connection):
    conn, db = establish_connection
    query = "MATCH (a:person)-[e:knows]->(b:person) return a,b,e"
    pd = to_panda.to_panda(db, query, relation_name=['e'])
    assert pd.loc[0].tolist()[0] == datetime.date(2021, 6, 30)
    assert pd.loc[0].tolist()[-1] == ['2huh9y89fsfw23', '23nsihufhw723']
    assert pd.loc[1].tolist()[0] == datetime.date(2021, 6, 30)
    assert pd.loc[1].tolist()[-1] == ['fnioh8323aeweae34d', 'osd89e2ejshuih12']
    assert pd.loc[2].tolist()[0] == datetime.date(2021, 6, 30)
    assert pd.loc[2].tolist()[-1] == ['fwewe']


def test_to_panda_with_col_name(establish_connection):
    conn, db = establish_connection
    query = "MATCH (a:person) return a"
    pd = to_panda.to_panda(db, query, col_name=['a.age', 'a.fName'])
    assert pd.loc[0].tolist() == [35, 'Alice']
    assert pd.loc[1].tolist() == [30, 'Bob']
    assert pd.loc[2].tolist() == [45, 'Carol']
    assert pd.loc[3].tolist() == [20, 'Dan']
    assert pd.loc[4].tolist() == [20, 'Elizabeth']
    assert pd.loc[5].tolist() == [25, 'Farooq']
    assert pd.loc[6].tolist() == [40, 'Greg']
    assert pd.loc[7].tolist() == [83, 'Hubert Blaine Wolfeschlegelsteinhausenbergerdorff']


def test_to_panda_with_node_relation_col_name(establish_connection):
    conn, db = establish_connection
    query = "MATCH (a:person)-[e:knows]->(b:person) return a,e,b"
    pd = to_panda.to_panda(db, query, col_name=['b.age', 'b.fName'], node_name=['a'], relation_name=['e'])
    assert pd.loc[0].tolist()[0:6] == [2, 'Bob', 2, True, False, 30]
    assert pd.loc[0].tolist()[-3:-1] == [['2huh9y89fsfw23', '23nsihufhw723'], 35]
    assert pd.loc[1].tolist()[0:6] == [3, 'Carol', 1, False, True, 45]
    assert pd.loc[1].tolist()[-3:-1] == [['fnioh8323aeweae34d', 'osd89e2ejshuih12'], 35]
    assert pd.loc[2].tolist()[0:6] == [5, 'Dan', 2, False, True, 20]
    assert pd.loc[2].tolist()[-3:-1] == [['fwewe'], 35]
    assert pd.loc[3].tolist()[0:6] == [0, 'Alice', 1, True, False, 35]
    assert pd.loc[3].tolist()[-3:-1] == [['rnme', 'm8sihsdnf2990nfiwf'], 30]
    assert pd.loc[4].tolist()[0:6] == [3, 'Carol', 1, False, True, 45]
    assert pd.loc[4].tolist()[-3:-1] == [['fwh983-sdjisdfji', 'ioh89y32r2huir'], 30]
    assert pd.loc[5].tolist()[0:6] == [5, 'Dan', 2, False, True, 20]
    assert pd.loc[5].tolist()[-3:-1] == [['fewh9182912e3', 'h9y8y89soidfsf', 'nuhudf78w78efw', 'hioshe0f9023sdsd'], 30]
    assert pd.loc[6].tolist()[0:6] == [0, 'Alice', 1, True, False, 35]
    assert pd.loc[6].tolist()[-3:-1] == [['njnojppo9u0jkmf', 'fjiojioh9h9h89hph'], 45]
    assert pd.loc[7].tolist()[0:6] == [2, 'Bob', 2, True, False, 30]
    assert pd.loc[7].tolist()[-3:-1] == [['fwehu9h9832wewew', '23u9h989sdfsss'], 45]
