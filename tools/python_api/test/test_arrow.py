import sys

sys.path.append('../build/')
import kuzu
import pyarrow as pa
import datetime
import ground_truth


def test_to_arrow(establish_connection):
    conn, db = establish_connection

    def _test_primitive_data_types(_conn):
        query = "MATCH (a:person) RETURN a.age, to_int32(a.age), to_int16(a.age), a.isStudent, a.eyeSight, a.birthdate, a.registerTime," \
                " a.lastJobDuration, a.fName ORDER BY a.ID"
        arrow_tbl = _conn.execute(query).get_as_arrow(8)
        assert arrow_tbl.num_columns == 9

        age_col = arrow_tbl.column(0)
        assert age_col.type == pa.int64()
        assert age_col.length() == 8
        assert age_col.to_pylist() == [35, 30, 45, 20, 20, 25, 40, 83]

        age_32_col = arrow_tbl.column(1)
        assert age_32_col.type == pa.int32()
        assert age_32_col.length() == 8
        assert age_32_col.to_pylist() == [35, 30, 45, 20, 20, 25, 40, 83]

        age_16_col = arrow_tbl.column(2)
        assert age_16_col.type == pa.int16()
        assert age_16_col.length() == 8
        assert age_16_col.to_pylist() == [35, 30, 45, 20, 20, 25, 40, 83]

        is_student_col = arrow_tbl.column(3)
        assert is_student_col.type == pa.bool_()
        assert is_student_col.length() == 8
        assert is_student_col.to_pylist() == [True, True, False, False, False, True, False, False]

        eye_sight_col = arrow_tbl.column(4)
        assert eye_sight_col.type == pa.float64()
        assert eye_sight_col.length() == 8
        assert eye_sight_col.to_pylist() == [5.0, 5.1, 5.0, 4.8, 4.7, 4.5, 4.9, 4.9]

        birthdate_col = arrow_tbl.column(5)
        assert birthdate_col.type == pa.date32()
        assert birthdate_col.length() == 8
        assert birthdate_col.to_pylist() == [datetime.date(1900, 1, 1), datetime.date(1900, 1, 1),
                                             datetime.date(1940, 6, 22), datetime.date(1950, 7, 23),
                                             datetime.date(1980, 10, 26), datetime.date(1980, 10, 26),
                                             datetime.date(1980, 10, 26), datetime.date(1990, 11, 27)]

        register_time_col = arrow_tbl.column(6)
        assert register_time_col.type == pa.timestamp('us')
        assert register_time_col.length() == 8
        assert register_time_col.to_pylist() == [
            datetime.datetime(2011, 8, 20, 11, 25, 30), datetime.datetime(2008, 11, 3, 15, 25, 30, 526),
            datetime.datetime(1911, 8, 20, 2, 32, 21), datetime.datetime(2031, 11, 30, 12, 25, 30),
            datetime.datetime(1976, 12, 23, 11, 21, 42), datetime.datetime(1972, 7, 31, 13, 22, 30, 678559),
            datetime.datetime(1976, 12, 23, 4, 41, 42), datetime.datetime(2023, 2, 21, 13, 25, 30)]

        last_job_duration_col = arrow_tbl.column(7)
        assert last_job_duration_col.type == pa.duration('ms')
        assert last_job_duration_col.length() == 8
        assert last_job_duration_col.to_pylist() == [datetime.timedelta(days=99, seconds=36334, microseconds=628000),
                                                     datetime.timedelta(days=543, seconds=4800),
                                                     datetime.timedelta(microseconds=125000),
                                                     datetime.timedelta(days=541, seconds=57600, microseconds=24000),
                                                     datetime.timedelta(0),
                                                     datetime.timedelta(days=2016, seconds=68600),
                                                     datetime.timedelta(microseconds=125000),
                                                     datetime.timedelta(days=541, seconds=57600, microseconds=24000)]

        f_name_col = arrow_tbl.column(8)
        assert f_name_col.type == pa.string()
        assert f_name_col.length() == 8
        assert f_name_col.to_pylist() == ["Alice", "Bob", "Carol", "Dan", "Elizabeth", "Farooq", "Greg",
                                          "Hubert Blaine Wolfeschlegelsteinhausenbergerdorff"]

    def _test_utf8_string(_conn):
        query = "MATCH (m:movies) RETURN m.name"
        query_result = _conn.execute(query)

        arrow_tbl = query_result.get_as_arrow(3)
        assert arrow_tbl.num_columns == 1
        name_col = arrow_tbl.column(0)
        assert name_col.type == pa.string()
        assert name_col.length() == 3
        assert name_col.to_pylist() == ["Sóló cón tu párejâ", "The 😂😃🧘🏻‍♂️🌍🌦️🍞🚗 movie", "Roma"]

    def _test_in_small_chunk_size(_conn):
        query = "MATCH (a:person) RETURN a.age, a.fName ORDER BY a.ID"
        query_result = _conn.execute(query)

        arrow_tbl = query_result.get_as_arrow(4)
        assert arrow_tbl.num_columns == 2
        age_col = arrow_tbl.column(0)
        assert age_col.type == pa.int64()
        assert age_col.length() == 8
        f_name_col = arrow_tbl.column(1)
        assert f_name_col.type == pa.string()
        assert f_name_col.length() == 8

        assert age_col.to_pylist() == [35, 30, 45, 20, 20, 25, 40, 83]
        assert f_name_col.to_pylist() == ["Alice", "Bob", "Carol", "Dan", "Elizabeth", "Farooq", "Greg",
                                          "Hubert Blaine Wolfeschlegelsteinhausenbergerdorff"]

    def _test_with_nulls(_conn):
        query = "MATCH (a:person:organisation) RETURN label(a), a.fName, a.orgCode ORDER BY a.ID"
        query_result = _conn.execute(query)
        arrow_tbl = query_result.get_as_arrow(12)
        assert arrow_tbl.num_columns == 3
        label_col = arrow_tbl.column(0)
        assert label_col.type == pa.string()
        assert label_col.length() == 11
        assert label_col.to_pylist() == ["person", "organisation", "person", "person", "organisation", "person",
                                         "organisation", "person", "person", "person", "person"]

        f_name_col = arrow_tbl.column(1)
        assert f_name_col.type == pa.string()
        assert f_name_col.length() == 11
        assert f_name_col.to_pylist() == ["Alice", None, "Bob", "Carol", None, "Dan", None, "Elizabeth", "Farooq",
                                          "Greg", "Hubert Blaine Wolfeschlegelsteinhausenbergerdorff"]

        org_code_col = arrow_tbl.column(2)
        assert org_code_col.type == pa.int64()
        assert org_code_col.length() == 11
        assert org_code_col.to_pylist() == [None, 325, None, None, 934, None, 824, None, None, None, None]

    _test_primitive_data_types(conn)
    _test_utf8_string(conn)
    _test_in_small_chunk_size(conn)
    _test_with_nulls(conn)


# TODO: enable this test once we support fixed_size_list
# def test_to_arrow_complex(establish_connection):
#     conn, db = establish_connection
#
#     def _test_node(_conn):
#         query = "MATCH (p:person) RETURN p ORDER BY p.ID"
#         query_result = _conn.execute(query)
#         arrow_tbl = query_result.get_as_arrow(12)
#         p_col = arrow_tbl.column(0)
#
#         assert p_col.to_pylist() == [ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[0],
#                                      ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[2],
#                                      ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[3],
#                                      ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[5],
#                                      ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[7],
#                                      ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[8],
#                                      ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[9],
#                                      ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[10]]
#
#     def _test_node_rel(_conn):
#         query = "MATCH (a:person)-[e:workAt]->(b:organisation) RETURN a, e, b;"
#         query_result = _conn.execute(query)
#         arrow_tbl = query_result.get_as_arrow(12)
#         assert arrow_tbl.num_columns == 3
#         a_col = arrow_tbl.column(0)
#         assert a_col.length() == 3
#         e_col = arrow_tbl.column(1)
#         assert a_col.length() == 3
#         b_col = arrow_tbl.column(2)
#         assert a_col.length() == 3
#         assert a_col.to_pylist() == [ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[3],
#                                      ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[5],
#                                      ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[7]]
#         assert e_col.to_pylist() == [
#             {'_src': {'offset': 2, 'tableID': 0}, '_dst': {'offset': 1, 'tableID': 2},
#              '_id': {'offset': 0, 'tableID': 4}, 'year': 2015},
#             {'_src': {'offset': 3, 'tableID': 0}, '_dst': {'offset': 2, 'tableID': 2},
#              '_id': {'offset': 1, 'tableID': 4}, 'year': 2010},
#             {'_src': {'offset': 4, 'tableID': 0}, '_dst': {'offset': 2, 'tableID': 2},
#              '_id': {'offset': 2, 'tableID': 4}, 'year': 2015}]
#         assert b_col.to_pylist() == [ground_truth.TINY_SNB_ORGANISATIONS_GROUND_TRUTH[4],
#                                      ground_truth.TINY_SNB_ORGANISATIONS_GROUND_TRUTH[6],
#                                      ground_truth.TINY_SNB_ORGANISATIONS_GROUND_TRUTH[6]]
#
#     _test_node(conn)
#     _test_node_rel(conn)
