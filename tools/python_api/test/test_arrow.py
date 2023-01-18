import sys

sys.path.append('../build/')
import kuzu
import pyarrow as pa
import datetime


def test_to_arrow(establish_connection):
    conn, db = establish_connection

    def _test_to_arrow(conn):
        query = "MATCH (a:person) RETURN a.age, a.isStudent, a.eyeSight, a.birthdate, a.registerTime, a.lastJobDuration ORDER BY a.ID"
        arrow_tbl = conn.execute(query).get_as_arrow(8)
        assert arrow_tbl.num_columns == 6

        age_col = arrow_tbl.column(0)
        assert age_col.type == pa.int64()
        assert age_col.length() == 8
        assert age_col.to_pylist() == [35, 30, 45, 20, 20, 25, 40, 83]

        is_student_col = arrow_tbl.column(1)
        assert is_student_col.type == pa.bool_()
        assert is_student_col.length() == 8
        assert is_student_col.to_pylist() == [True, True, False, False, False, True, False, False]

        eye_sight_col = arrow_tbl.column(2)
        assert eye_sight_col.type == pa.float64()
        assert eye_sight_col.length() == 8
        assert eye_sight_col.to_pylist() == [5.0, 5.1, 5.0, 4.8, 4.7, 4.5, 4.9, 4.9]

        birthdate_col = arrow_tbl.column(3)
        assert birthdate_col.type == pa.date32()
        assert birthdate_col.length() == 8
        assert birthdate_col.to_pylist() == [datetime.date(1900, 1, 1), datetime.date(1900, 1, 1),
                                           datetime.date(1940, 6, 22), datetime.date(1950, 7, 23),
                                           datetime.date(1980, 10, 26), datetime.date(1980, 10, 26),
                                           datetime.date(1980, 10, 26), datetime.date(1990, 11, 27)]

        register_time_col = arrow_tbl.column(4)
        assert register_time_col.type == pa.timestamp('us')
        assert register_time_col.length() == 8
        assert register_time_col.to_pylist() == [
            datetime.datetime(2011, 8, 20, 11, 25, 30), datetime.datetime(2008, 11, 3, 15, 25, 30, 526),
            datetime.datetime(1911, 8, 20, 2, 32, 21), datetime.datetime(2031, 11, 30, 12, 25, 30),
            datetime.datetime(1976, 12, 23, 11, 21, 42), datetime.datetime(1972, 7, 31, 13, 22, 30, 678559),
            datetime.datetime(1976, 12, 23, 4, 41, 42), datetime.datetime(2023, 2, 21, 13, 25, 30)]

        last_job_duration_col = arrow_tbl.column(5)
        assert last_job_duration_col.type == pa.duration('ms')
        assert last_job_duration_col.length() == 8
        assert last_job_duration_col.to_pylist() == [datetime.timedelta(days=99, seconds=36334, microseconds=628000),
                                                   datetime.timedelta(days=543, seconds=4800),
                                                   datetime.timedelta(microseconds=125000),
                                                   datetime.timedelta(days=541, seconds=57600, microseconds=24000),
                                                   datetime.timedelta(0), datetime.timedelta(days=2016, seconds=68600),
                                                   datetime.timedelta(microseconds=125000),
                                                   datetime.timedelta(days=541, seconds=57600, microseconds=24000)]

    _test_to_arrow(conn)
