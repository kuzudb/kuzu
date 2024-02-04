import datetime
import pytz
from uuid import UUID

def test_bool(establish_connection):
    conn, db = establish_connection
    result = conn.execute(
        "MATCH (a:person) WHERE a.ID = 0 RETURN a.isStudent;")
    assert result.has_next()
    assert result.get_next() == [True]
    assert not result.has_next()
    result.close()


def test_int(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.age;")
    assert result.has_next()
    assert result.get_next() == [35]
    assert not result.has_next()
    result.close()

def test_int8(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.level;")
    assert result.has_next()
    assert result.get_next() == [5]
    assert not result.has_next()
    result.close()

def test_uint8(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.ulevel;")
    assert result.has_next()
    assert result.get_next() == [250]
    assert not result.has_next()
    result.close()

def test_uint16(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.ulength;")
    assert result.has_next()
    assert result.get_next() == [33768]
    assert not result.has_next()
    result.close()

def test_uint32(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.temprature;")
    assert result.has_next()
    assert result.get_next() == [32800]
    assert not result.has_next()
    result.close()

def test_uint64(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.code;")
    assert result.has_next()
    assert result.get_next() == [9223372036854775808]
    assert not result.has_next()
    result.close()

def test_int128(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.hugedata;")
    assert result.has_next()
    assert result.get_next() == [1844674407370955161811111111]
    assert not result.has_next()
    result.close()

def test_serial(establish_connection):
     conn, db = establish_connection
     result = conn.execute("MATCH (a:moviesSerial) WHERE a.ID = 2 RETURN a.ID;")
     assert result.has_next()
     assert result.get_next() == [2]
     assert not result.has_next()
     result.close()


def test_double(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.eyeSight;")
    assert result.has_next()
    assert result.get_next() == [5.0]
    assert not result.has_next()
    result.close()


def test_string(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.fName;")
    assert result.has_next()
    assert result.get_next() == ['Alice']
    assert not result.has_next()
    result.close()


def test_blob(establish_connection):
    conn, db = establish_connection
    result = conn.execute("RETURN BLOB('\\\\xAA\\\\xBB\\\\xCD\\\\x1A')")
    assert result.has_next()
    result_blob = result.get_next()[0]
    assert len(result_blob) == 4
    assert result_blob[0] == 0xAA
    assert result_blob[1] == 0xBB
    assert result_blob[2] == 0xCD
    assert result_blob[3] == 0x1A
    assert not result.has_next()
    result.close()

def test_uuid(establish_connection):
    conn, db = establish_connection
    result = conn.execute("RETURN UUID('A0EEBC99-9c0b-4ef8-bb6d-6bb9bd380a12')")
    assert result.has_next()
    assert result.get_next() == [UUID('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12')]
    assert not result.has_next()
    result.close()

def test_date(establish_connection):
    conn, db = establish_connection
    result = conn.execute(
        "MATCH (a:person) WHERE a.ID = 0 RETURN a.birthdate;")
    assert result.has_next()
    assert result.get_next() == [datetime.date(1900, 1, 1)]
    assert not result.has_next()
    result.close()


def test_timestamp(establish_connection):
    conn, db = establish_connection
    result = conn.execute(
        "MATCH (a:person) WHERE a.ID = 0 RETURN a.registerTime;")
    assert result.has_next()
    assert result.get_next() == [datetime.datetime(2011, 8, 20, 11, 25, 30)]
    assert not result.has_next()
    result.close()

def test_timestamp_tz(establish_connection):
    conn, db = establish_connection
    result = conn.execute(
        "MATCH (a:movies) WHERE a.length = 126 RETURN a.description.release_tz;")
    assert result.has_next()
    assert result.get_next() == [datetime.datetime(2011, 8, 20, 11, 25, 30, 123456, pytz.UTC)]
    assert not result.has_next()
    result.close()

def test_timestamp_ns(establish_connection):
    conn, db = establish_connection
    result = conn.execute(
        "MATCH (a:movies) WHERE a.length = 126 RETURN a.description.release_ns;")
    assert result.has_next()
    assert result.get_next() == [datetime.datetime(2011, 8, 20, 11, 25, 30, 123456)]
    assert not result.has_next()
    result.close()

def test_timestamp_ms(establish_connection):
    conn, db = establish_connection
    result = conn.execute(
        "MATCH (a:movies) WHERE a.length = 126 RETURN a.description.release_ms;")
    assert result.has_next()
    assert result.get_next() == [datetime.datetime(2011, 8, 20, 11, 25, 30, 123000)]
    assert not result.has_next()
    result.close()

def test_timestamp_ns(establish_connection):
    conn, db = establish_connection
    result = conn.execute(
        "MATCH (a:movies) WHERE a.length = 126 RETURN a.description.release_sec;")
    assert result.has_next()
    assert result.get_next() == [datetime.datetime(2011, 8, 20, 11, 25, 30)]
    assert not result.has_next()
    result.close()

def test_interval(establish_connection):
    conn, db = establish_connection
    result = conn.execute(
        "MATCH (a:person) WHERE a.ID = 0 RETURN a.lastJobDuration;")
    assert result.has_next()
    assert result.get_next() == [datetime.timedelta(days=1082, seconds=46920)]
    assert not result.has_next()
    result.close()


def test_list(establish_connection):
    conn, db = establish_connection
    result = conn.execute(
        "MATCH (a:person) WHERE a.ID = 0 RETURN a.courseScoresPerTerm;")
    assert result.has_next()
    assert result.get_next() == [[[10, 8], [6, 7, 8]]]
    assert not result.has_next()
    result.close()


def test_map(establish_connection):
    conn, db = establish_connection
    result = conn.execute(
        "MATCH (m:movies) WHERE m.length = 2544 RETURN m.audience;")
    assert result.has_next()
    assert result.get_next() == [{'audience1': 33}]
    assert not result.has_next()
    result.close()


def test_union(establish_connection):
    conn, db = establish_connection
    result = conn.execute(
        "MATCH (m:movies) WHERE m.length = 2544 RETURN m.grade;")
    assert result.has_next()
    assert result.get_next() == [8.989]
    assert not result.has_next()
    result.close()


def test_node(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a")
    assert result.has_next()
    n = result.get_next()
    assert (len(n) == 1)
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
    assert (r['_label'] == 'workAt')
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
    assert (description['rating'] == 1223)
    assert (description['views'] == 10003)
    assert (description['release'] ==
            datetime.datetime(2011, 2, 11, 16, 44, 22))
    assert (description['release_ns'] ==
            datetime.datetime(2011, 2, 11, 16, 44, 22, 123456))
    assert (description['release_ms'] ==
            datetime.datetime(2011, 2, 11, 16, 44, 22, 123000))
    assert (description['release_sec'] ==
            datetime.datetime(2011, 2, 11, 16, 44, 22))
    assert (description['release_tz'] ==
            datetime.datetime(2011, 2, 11, 16, 44, 22, 123456, pytz.UTC))
    assert (description['film'] == datetime.date(2013, 2, 22))
    assert (description['stars'] == 100)
    assert (description['u8'] == 1)
    assert (description['u16'] == 15)
    assert (description['u32'] == 200)
    assert (description['u64'] == 4)
    assert (description['hugedata'] == -15)
    assert not result.has_next()
    result.close()


def test_recursive_rel(establish_connection):
    conn, db = establish_connection
    result = conn.execute(
        "MATCH (a:person)-[e:studyAt*1..1]->(b:organisation) WHERE a.fName = 'Alice' RETURN e;"
    )
    assert result.has_next()
    n = result.get_next()
    assert (len(n) == 1)
    e = n[0]
    assert ("_nodes" in e)
    assert ("_rels" in e)
    assert (len(e["_nodes"]) == 0)
    assert (len(e["_rels"]) == 1)
    rel = e["_rels"][0]
    excepted_rel = {'_src': {'offset': 0, 'table': 0},
                    '_dst': {'offset': 0, 'table': 1},
                    '_label': 'studyAt',
                    'year': 2021,
                    'places': ['wwAewsdndweusd', 'wek'],
                    'length': 5, 'level': 5, 'code': 9223372036854775808, 'temprature':32800,
                    'ulength':33768, 'ulevel': 250, 'hugedata': 1844674407370955161811111111,
                    }
    assert (rel == excepted_rel)
    assert not result.has_next()
    result.close()

def test_rdf_variant(establish_connection):
    conn, db = establish_connection
    result = conn.execute("MATCH (a:T_l) RETURN a.val ORDER BY a.id")
    assert result.has_next()
    assert result.get_next() == [12]
    assert result.has_next()
    assert result.get_next() == [43]
    assert result.has_next()
    assert result.get_next() == [33]
    assert result.has_next()
    assert result.get_next() == [2]
    assert result.has_next()
    assert result.get_next() == [90]
    assert result.has_next()
    assert result.get_next() == [77]
    assert result.has_next()
    assert result.get_next() == [12]
    assert result.has_next()
    assert result.get_next() == [1]
    assert result.has_next()
    float_result = result.get_next()[0]
    assert abs(float_result - 4.4) < 0.00001
    assert result.has_next()
    float_result = result.get_next()[0]
    assert abs(float_result - 1.2) < 0.00001
    assert result.has_next()
    assert result.get_next() == [True]
    assert result.has_next()
    assert result.get_next() == ["hhh"]
    assert result.has_next()
    assert result.get_next() == [datetime.date(2024, 1, 1)]
    assert result.has_next()
    assert result.get_next() == [datetime.datetime(2024, 1, 1, 11, 25, 30)]
    assert result.has_next()
    assert result.get_next() == [datetime.timedelta(days=2)]
    assert result.has_next()
    result_blob = result.get_next()[0]
    assert len(result_blob) == 1
    assert result_blob[0] == 0xB2
    assert not result.has_next()
    result.close()
    result = conn.execute("DROP RDFGraph T;")
    result.close()
