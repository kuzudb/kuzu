import datetime


def test_bool_wrap(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.isStudent;")
    assert result.has_next()
    assert result.get_next() == [True]
    assert not result.has_next()
    result.close()


def test_int_wrap(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.age;")
    assert result.has_next()
    assert result.get_next() == [35]
    assert not result.has_next()
    result.close()


def test_double_wrap(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.eyeSight;")
    assert result.has_next()
    assert result.get_next() == [5.0]
    assert not result.has_next()
    result.close()


def test_string_wrap(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.fName;")
    assert result.has_next()
    assert result.get_next() == ['Alice']
    assert not result.has_next()
    result.close()


def test_date_wrap(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.birthdate;")
    assert result.has_next()
    assert result.get_next() == [datetime.date(1900, 1, 1)]
    assert not result.has_next()
    result.close()


def test_timestamp_wrap(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.registerTime;")
    assert result.has_next()
    assert result.get_next() == [datetime.datetime(2011, 8, 20, 11, 25, 30)]
    assert not result.has_next()
    result.close()


def test_interval_wrap(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.lastJobDuration;")
    assert result.has_next()
    assert result.get_next() == [datetime.timedelta(days=1082, seconds=46920)]
    assert not result.has_next()
    result.close()


def test_list_wrap(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.courseScoresPerTerm;")
    assert result.has_next()
    assert result.get_next() == [[[10, 8], [6, 7, 8]]]
    assert not result.has_next()
    result.close()


def test_node(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a")
    assert result.has_next()
    n = result.get_next()
    assert(len(n) == 1)
    n = n[0]
    assert (n['_label'] == 'person')
    assert (n['ID'] == 0)
    assert (n['fName'] == 'Alice')
    assert (n['gender'] == 1)
    assert (n['isStudent'] == True)
    assert (n['eyeSight'] == 5.0)
    assert (n['birthdate'] == datetime.date(1900, 1, 1))
    assert (n['registerTime'] == datetime.datetime(2011, 8, 20, 11, 25, 30))
    assert (n['lastJobDuration'] == datetime.timedelta(
        days=1082, seconds=46920))
    assert (n['courseScoresPerTerm'] == [[10, 8], [6, 7, 8]])
    assert (n['usedNames'] == ['Aida'])
    assert not result.has_next()
    result.close()


def test_rel(establish_connection):
    conn, db = establish_connection
    result = conn.execute(
        "MATCH (p:person)-[r:workAt]->(o:organisation) WHERE p.ID = 5 RETURN p, r, o")
    assert result.has_next()
    n = result.get_next()
    assert (len(n) == 3)
    p = n[0]
    r = n[1]
    o = n[2]
    assert (p['_label'] == 'person')
    assert (p['ID'] == 5)
    assert (o['_label'] == 'organisation')
    assert (o['ID'] == 6)
    assert (r['year'] == 2010)
    assert (r['_src'] == p['_id'])
    assert (r['_dst'] == o['_id'])
    assert not result.has_next()
    result.close()


def test_struct(establish_connection):
    conn, db = establish_connection
    result = conn.execute(
        'MATCH (m:movies) WHERE m.name="Roma" RETURN m.description')
    assert result.has_next()
    n = result.get_next()
    assert (len(n) == 1)
    description = n[0]
    print(description)
    assert (description['RATING'] == 1223)
    assert (description['VIEWS'] == 10003)
    assert (description['RELEASE'] ==
            datetime.datetime(2011, 2, 11, 16, 44, 22))
    assert (description['FILM'] == datetime.date(2013, 2, 22))
    assert not result.has_next()
    result.close()
