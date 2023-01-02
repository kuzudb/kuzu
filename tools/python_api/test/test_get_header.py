def test_get_column_names(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person)-[e:knows]->(b:person) RETURN a.fName, e.date, b.ID;")
    column_names = result.get_column_names()
    assert column_names[0] == 'a.fName'
    assert column_names[1] == 'e.date'
    assert column_names[2] == 'b.ID'
    result.close()


def test_get_column_data_types(establish_connection):
    conn, db = establish_connection
    result = conn.execute(
        "MATCH (p:person) RETURN p.ID, p.fName, p.isStudent, p.eyeSight, p.birthdate, p.registerTime, "
        "p.lastJobDuration, p.workedHours, p.courseScoresPerTerm;")
    column_data_types = result.get_column_data_types()
    assert column_data_types[0] == 'INT64'
    assert column_data_types[1] == 'STRING'
    assert column_data_types[2] == 'BOOL'
    assert column_data_types[3] == 'DOUBLE'
    assert column_data_types[4] == 'DATE'
    assert column_data_types[5] == 'TIMESTAMP'
    assert column_data_types[6] == 'INTERVAL'
    assert column_data_types[7] == 'INT64[]'
    assert column_data_types[8] == 'INT64[][]'
    result.close()
