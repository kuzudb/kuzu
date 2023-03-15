import numpy as np
import datetime
import sys

sys.path.append('../build/')
import kuzu
from pandas import Timestamp, Timedelta, isna


def test_to_df(establish_connection):
    conn, db = establish_connection

    def _test_to_df(conn):
        query = "MATCH (p:person) return " \
                "p.ID, p.fName, p.gender, p.isStudent, p.eyeSight, p.birthdate, p.registerTime, " \
                "p.lastJobDuration, p.workedHours, p.usedNames, p.courseScoresPerTerm ORDER BY p.ID"
        pd = conn.execute(query).get_as_df()
        assert pd['p.ID'].tolist() == [0, 2, 3, 5, 7, 8, 9, 10]
        assert str(pd['p.ID'].dtype) == "int64"
        assert pd['p.fName'].tolist() == ["Alice", "Bob", "Carol", "Dan", "Elizabeth", "Farooq", "Greg",
                                          "Hubert Blaine Wolfeschlegelsteinhausenbergerdorff"]
        assert str(pd['p.fName'].dtype) == "object"
        assert pd['p.gender'].tolist() == [1, 2, 1, 2, 1, 2, 2, 2]
        assert str(pd['p.gender'].dtype) == "int64"
        assert pd['p.isStudent'].tolist() == [True, True, False, False, False, True, False, False]
        assert str(pd['p.isStudent'].dtype) == "bool"
        assert pd['p.eyeSight'].tolist() == [5.0, 5.1, 5.0, 4.8, 4.7, 4.5, 4.9, 4.9]
        assert str(pd['p.eyeSight'].dtype) == "float64"
        assert pd['p.birthdate'].tolist() == [Timestamp('1900-01-01'), Timestamp('1900-01-01'),
                                              Timestamp('1940-06-22'), Timestamp('1950-07-23 '),
                                              Timestamp('1980-10-26'), Timestamp('1980-10-26'),
                                              Timestamp('1980-10-26'), Timestamp('1990-11-27')]
        assert str(pd['p.birthdate'].dtype) == "datetime64[ns]"
        assert pd['p.registerTime'].tolist() == [Timestamp('2011-08-20 11:25:30'),
                                                 Timestamp('2008-11-03 15:25:30.000526'),
                                                 Timestamp('1911-08-20 02:32:21'), Timestamp('2031-11-30 12:25:30'),
                                                 Timestamp('1976-12-23 11:21:42'),
                                                 Timestamp('1972-07-31 13:22:30.678559'),
                                                 Timestamp('1976-12-23 04:41:42'), Timestamp('2023-02-21 13:25:30')]
        assert str(pd['p.registerTime'].dtype) == "datetime64[ns]"
        assert pd['p.lastJobDuration'].tolist() == [Timedelta('1082 days 13:02:00'),
                                                    Timedelta('3750 days 13:00:00.000024'),
                                                    Timedelta('2 days 00:24:11'),
                                                    Timedelta('3750 days 13:00:00.000024'),
                                                    Timedelta('2 days 00:24:11'), Timedelta('0 days 00:18:00.024000'),
                                                    Timedelta('3750 days 13:00:00.000024'),
                                                    Timedelta('1082 days 13:02:00')]
        assert str(pd['p.lastJobDuration'].dtype) == "timedelta64[ns]"
        assert pd['p.workedHours'].tolist() == [[10, 5], [12, 8], [4, 5], [1, 9], [2], [3, 4, 5, 6, 7], [1],
                                                [10, 11, 12, 3, 4, 5, 6, 7]]
        assert str(pd['p.workedHours'].dtype) == "object"
        assert pd['p.workedHours'].tolist() == [[10, 5], [12, 8], [4, 5], [1, 9], [2], [3, 4, 5, 6, 7], [1],
                                                [10, 11, 12, 3, 4, 5, 6, 7]]
        assert str(pd['p.workedHours'].dtype) == "object"
        assert pd['p.usedNames'].tolist() == [["Aida"], ['Bobby'], ['Carmen', 'Fred'],
                                              ['Wolfeschlegelstein', 'Daniel'], ['Ein'], ['Fesdwe'], ['Grad'],
                                              ['Ad', 'De', 'Hi', 'Kye', 'Orlan']]
        assert str(pd['p.usedNames'].dtype) == "object"
        assert pd['p.courseScoresPerTerm'].tolist() == [[[10, 8], [6, 7, 8]], [[8, 9], [9, 10]], [[8, 10]],
                                                        [[7, 4], [8, 8], [9]], [[6], [7], [8]], [[8]], [[10]],
                                                        [[7], [10], [6, 7]]]
        assert str(pd['p.courseScoresPerTerm'].dtype) == "object"

    _test_to_df(conn)

    conn.set_max_threads_for_exec(2)
    _test_to_df(conn)

    conn.set_max_threads_for_exec(1)
    _test_to_df(conn)

    db.set_logging_level("debug")
    _test_to_df(conn)

    db.set_logging_level("info")
    _test_to_df(conn)

    db.set_logging_level("err")
    _test_to_df(conn)


def test_df_multiple_times(establish_connection):
    conn, _ = establish_connection
    query = "MATCH (p:person) return p.ID ORDER BY p.ID"
    res = conn.execute(query)
    df = res.get_as_df()
    df_2 = res.get_as_df()
    df_3 = res.get_as_df()
    assert df['p.ID'].tolist() == [0, 2, 3, 5, 7, 8, 9, 10]
    assert df_2['p.ID'].tolist() == [0, 2, 3, 5, 7, 8, 9, 10]
    assert df_3['p.ID'].tolist() == [0, 2, 3, 5, 7, 8, 9, 10]


def test_df_get_node(establish_connection):
    conn, _ = establish_connection
    query = "MATCH (p:person) return p"

    res = conn.execute(query)
    df = res.get_as_df()
    p_list = df['p'].tolist()
    assert len(p_list) == 8

    ground_truth = {
        'ID': [0, 2, 3, 5, 7, 8, 9, 10],
        'fName': ["Alice", "Bob", "Carol", "Dan", "Elizabeth", "Farooq", "Greg",
                  "Hubert Blaine Wolfeschlegelsteinhausenbergerdorff"],
        'gender': [1, 2, 1, 2, 1, 2, 2, 2],
        'isStudent': [True, True, False, False, False, True, False, False],
        'eyeSight': [5.0, 5.1, 5.0, 4.8, 4.7, 4.5, 4.9, 4.9],
        'birthdate': [datetime.date(1900, 1, 1), datetime.date(1900, 1, 1),
                      datetime.date(1940, 6, 22), datetime.date(1950, 7, 23),
                      datetime.date(1980, 10, 26), datetime.date(1980, 10, 26),
                      datetime.date(1980, 10, 26), datetime.date(1990, 11, 27)],
        'registerTime': [Timestamp('2011-08-20 11:25:30'),
                         Timestamp('2008-11-03 15:25:30.000526'),
                         Timestamp(
                             '1911-08-20 02:32:21'), Timestamp('2031-11-30 12:25:30'),
                         Timestamp('1976-12-23 11:21:42'),
                         Timestamp('1972-07-31 13:22:30.678559'),
                         Timestamp('1976-12-23 04:41:42'), Timestamp('2023-02-21 13:25:30')],
        'lastJobDuration': [Timedelta('1082 days 13:02:00'),
                            Timedelta('3750 days 13:00:00.000024'),
                            Timedelta('2 days 00:24:11'),
                            Timedelta('3750 days 13:00:00.000024'),
                            Timedelta('2 days 00:24:11'), Timedelta(
                '0 days 00:18:00.024000'),
                            Timedelta('3750 days 13:00:00.000024'),
                            Timedelta('1082 days 13:02:00')],
        'workedHours': [[10, 5], [12, 8], [4, 5], [1, 9], [2], [3, 4, 5, 6, 7], [1],
                        [10, 11, 12, 3, 4, 5, 6, 7]],
        'usedNames': [["Aida"], ['Bobby'], ['Carmen', 'Fred'],
                      ['Wolfeschlegelstein', 'Daniel'], [
                          'Ein'], ['Fesdwe'], ['Grad'],
                      ['Ad', 'De', 'Hi', 'Kye', 'Orlan']],
        'courseScoresPerTerm': [[[10, 8], [6, 7, 8]], [[8, 9], [9, 10]], [[8, 10]],
                                [[7, 4], [8, 8], [9]], [
                                    [6], [7], [8]], [[8]], [[10]],
                                [[7], [10], [6, 7]]],
        '_label': ['person', 'person', 'person', 'person', 'person', 'person',
                   'person', 'person'],
    }
    for i in range(len(p_list)):
        p = p_list[i]
        for key in ground_truth:
            assert p[key] == ground_truth[key][i]


def test_df_get_node_rel(establish_connection):
    conn, _ = establish_connection
    res = conn.execute(
        "MATCH (p:person)-[r:workAt]->(o:organisation) RETURN p, r, o")

    df = res.get_as_df()
    p_list = df['p'].tolist()
    o_list = df['o'].tolist()

    assert len(p_list) == 3
    assert len(o_list) == 3

    ground_truth_p = {
        'ID': [3, 5, 7],
        'fName': ["Carol", "Dan", "Elizabeth"],
        'gender': [1, 2, 1],
        'isStudent': [False, False, False],
        'eyeSight': [5.0, 4.8, 4.7],
        'birthdate': [datetime.date(1940, 6, 22), datetime.date(1950, 7, 23),
                      datetime.date(1980, 10, 26)],
        'registerTime': [Timestamp('1911-08-20 02:32:21'), Timestamp('2031-11-30 12:25:30'),
                         Timestamp('1976-12-23 11:21:42')
                         ],
        'lastJobDuration': [
            Timedelta('48 hours 24 minutes 11 seconds'),
            Timedelta('3750 days 13:00:00.000024'),
            Timedelta('2 days 00:24:11')],
        'workedHours': [[4, 5], [1, 9], [2]],
        'usedNames': [["Carmen", "Fred"], ['Wolfeschlegelstein', 'Daniel'], ['Ein']],
        'courseScoresPerTerm': [[[8, 10]], [[7, 4], [8, 8], [9]], [[6], [7], [8]]],
        '_label': ['person', 'person', 'person'],
    }
    for i in range(len(p_list)):
        p = p_list[i]
        for key in ground_truth_p:
            assert p[key] == ground_truth_p[key][i]

    ground_truth_o = {'ID': [4, 6, 6],
                      'name': ['CsWork', 'DEsWork', 'DEsWork'],
                      'orgCode': [934, 824, 824],
                      'mark': [4.1, 4.1, 4.1],
                      'score': [-100, 7, 7],
                      'history': ['2 years 4 days 10 hours', '2 years 4 hours 22 us 34 minutes',
                                  '2 years 4 hours 22 us 34 minutes'],
                      'licenseValidInterval': [Timedelta(days=9414),
                                               Timedelta(days=3, seconds=36000, microseconds=100000),
                                               Timedelta(days=3, seconds=36000, microseconds=100000)],
                      'rating': [0.78, 0.52, 0.52],
                      '_label': ['organisation', 'organisation', 'organisation'],
                      }
    for i in range(len(o_list)):
        o = df['o'][i]
        for key in ground_truth_o:
            assert o[key] == ground_truth_o[key][i]

    assert (df['r'][0]['year'] == 2015)
    assert (df['r'][1]['year'] == 2010)
    assert (df['r'][2]['year'] == 2015)

    for i in range(len(df['r'])):
        assert (df['r'][i]['_src'] == df['p'][i]['_id'])
        assert (df['r'][i]['_dst'] == df['o'][i]['_id'])
