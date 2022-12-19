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

