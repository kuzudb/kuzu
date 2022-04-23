import datetime


def test_bool_wrap(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.isStudent;")
    assert result.hasNext()
    assert result.getNext() == [True]
    assert not result.hasNext()
    result.close()


def test_int_wrap(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.age;")
    assert result.hasNext()
    assert result.getNext() == [35]
    assert not result.hasNext()
    result.close()


def test_double_wrap(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.eyeSight;")
    assert result.hasNext()
    assert result.getNext() == [5.0]
    assert not result.hasNext()
    result.close()


def test_string_wrap(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.fName;")
    assert result.hasNext()
    assert result.getNext() == ['Alice']
    assert not result.hasNext()
    result.close()


def test_date_wrap(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.birthdate;")
    assert result.hasNext()
    assert result.getNext() == [datetime.date(1900, 1, 1)]
    assert not result.hasNext()
    result.close()


def test_timestamp_wrap(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.registerTime;")
    assert result.hasNext()
    assert result.getNext() == [datetime.datetime(2011, 8, 20, 11, 25, 30)]
    assert not result.hasNext()
    result.close()


def test_interval_wrap(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.lastJobDuration;")
    assert result.hasNext()
    assert result.getNext() == [datetime.timedelta(days=1082, seconds=46920)]
    assert not result.hasNext()
    result.close()


def test_list_wrap(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.courseScoresPerTerm;")
    assert result.hasNext()
    assert result.getNext() == [[[10, 8], [6, 7, 8]]]
    assert not result.hasNext()
    result.close()

