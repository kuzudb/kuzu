import numpy as np
import sys
sys.path.append('../build/')
import _kuzu as kuzu
from pandas import Timestamp, Timedelta, isna

def test_to_df(establish_connection):
    conn, db = establish_connection

    def _test_to_df(conn):
        query = "MATCH (p:person) return * ORDER BY p.ID"
        pd = conn.execute(query).getAsDF()
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

    db.resize_buffer_manager(384 * 1024 * 1024)
    _test_to_df(conn)

    db.resize_buffer_manager(512 * 1024 * 1024)
    conn.set_max_threads_for_exec(4)
    _test_to_df(conn)

    db.set_logging_level(kuzu.loggingLevel.debug)
    _test_to_df(conn)

    db.set_logging_level(kuzu.loggingLevel.info)
    _test_to_df(conn)

    db.set_logging_level(kuzu.loggingLevel.err)
    _test_to_df(conn)
