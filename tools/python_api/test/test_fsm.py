from pathlib import Path

import kuzu
import pytest
from test_helper import KUZU_ROOT


def get_used_page_ranges(conn, table, column=None):
    used_pages = []
    if column is None:
        storage_info = conn.execute(f'call storage_info("{table}") return start_page_idx, num_pages')
    else:
        storage_info = conn.execute(
            f'call storage_info("{table}") where prefix(column_name, "{column}") return start_page_idx, num_pages'
        )
    while storage_info.has_next():
        cur_tuple = storage_info.get_next()
        # don't add empty (e.g. constant-compressed) pages
        if cur_tuple[1] > 0:
            used_pages.append(cur_tuple)
    return used_pages


def get_free_page_ranges(conn):
    free_pages = []
    result = conn.execute("call fsm_info() return *")
    while result.has_next():
        cur_tuple = result.get_next()
        assert cur_tuple[1] > 0
        free_pages.append(cur_tuple)
    return free_pages


def combine_adjacent_page_ranges(page_ranges):
    new_page_ranges = []
    last_range = None
    for page_range in page_ranges:
        if last_range is not None and last_range[0] + last_range[1] == page_range[0]:
            new_range = [last_range[0], last_range[1] + page_range[1]]
            last_range = new_range
        else:
            if last_range is not None:
                new_page_ranges.append(last_range)
            last_range = page_range
    if last_range:
        new_page_ranges.append(last_range)
    return new_page_ranges


def compare_page_range_lists(used_list, free_list):
    used_list.sort()
    free_list.sort()
    assert combine_adjacent_page_ranges(used_list) == free_list


def prevent_data_file_truncation(conn):
    conn.execute("CREATE NODE TABLE IF NOT EXISTS TMP(ID INT64, PRIMARY KEY(ID))")
    conn.execute("CREATE (:TMP{id: 1})")
    conn.execute("CREATE (:TMP{id: 2})")


@pytest.fixture
def fsm_node_table_setup(tmp_path: Path):
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    conn.execute("call threads=1")
    conn.execute("call auto_checkpoint=false")
    conn.execute(
        "create node table person (ID INt64, fName StRING, gender INT64, isStudent BoOLEAN, isWorker BOOLEAN, age INT64, eyeSight DOUBLE, birthdate DATE, registerTime TIMESTAMP, lastJobDuration interval, workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][], grades INT64[4], height float, u UUID, PRIMARY KEY (ID));"
    )
    conn.execute(
        f"COPY person FROM ['{KUZU_ROOT}/dataset/tinysnb/vPerson.csv', '{KUZU_ROOT}/dataset/tinysnb/vPerson2.csv'](ignore_errors=true, header=false)"
    )
    return db, conn


@pytest.fixture
def fsm_rel_table_setup(fsm_node_table_setup):
    _, conn = fsm_node_table_setup
    conn.execute(
        "create rel table knows (FROM person TO person, date DATE, meetTime TIMESTAMP, validInterval INTERVAL, comments STRING[], summary STRUCT(locations STRING[], transfer STRUCT(day DATE, amount INT64[])), notes UNION(firstmet DATE, type INT16, comment STRING), someMap MAP(STRING, STRING), MANY_MAnY);"
    )
    conn.execute(
        f"COPY knows FROM ['{KUZU_ROOT}/dataset/tinysnb/eKnows.csv', '{KUZU_ROOT}/dataset/tinysnb/eKnows_2.csv']"
    )
    return fsm_node_table_setup


@pytest.fixture
def fsm_rel_group_setup(tmp_path: Path):
    db = kuzu.Database(tmp_path)
    conn = kuzu.Connection(db)
    conn.execute("call threads=1")
    conn.execute("call auto_checkpoint=false")
    conn.execute("create node table personA (ID INt64, fName StRING, PRIMARY KEY (ID));")
    conn.execute("create node table personB (ID INt64, fName StRING, PRIMARY KEY (ID));")
    conn.execute("create rel table likes (FROM personA TO personB, FROM personB To personA, date DATE);")
    conn.execute(f'COPY personA FROM "{KUZU_ROOT}/dataset/rel-group/node.csv";')
    conn.execute(f'COPY personB FROM "{KUZU_ROOT}/dataset/rel-group/node.csv";')
    conn.execute(f'COPY likes FROM "{KUZU_ROOT}/dataset/rel-group/edge.csv" (FROM="personA", TO="personB");')
    conn.execute(f'COPY likes FROM "{KUZU_ROOT}/dataset/rel-group/edge.csv" (FROM="personB", TO="personA");')
    return db, conn


def test_fsm_reclaim_list_column(fsm_node_table_setup) -> None:
    _, conn = fsm_node_table_setup
    used_pages = get_used_page_ranges(conn, "person", "workedHours")
    conn.execute("alter table person drop workedHours")
    prevent_data_file_truncation(conn)
    conn.execute("checkpoint")
    free_pages = get_free_page_ranges(conn)
    compare_page_range_lists(used_pages, free_pages)


def test_fsm_reclaim_string_column(fsm_node_table_setup) -> None:
    _, conn = fsm_node_table_setup
    used_pages = get_used_page_ranges(conn, "person", "fName")
    conn.execute("alter table person drop fName")
    prevent_data_file_truncation(conn)
    conn.execute("checkpoint")
    free_pages = get_free_page_ranges(conn)
    compare_page_range_lists(used_pages, free_pages)


def test_fsm_reclaim_list_of_lists(fsm_node_table_setup) -> None:
    _, conn = fsm_node_table_setup
    used_pages = get_used_page_ranges(conn, "person", "courseScoresPerTerm")
    conn.execute("alter table person drop courseScoresPerTerm")
    prevent_data_file_truncation(conn)
    conn.execute("checkpoint")
    free_pages = get_free_page_ranges(conn)
    compare_page_range_lists(used_pages, free_pages)


def test_fsm_reclaim_node_table(fsm_node_table_setup) -> None:
    _, conn = fsm_node_table_setup
    used_pages = get_used_page_ranges(conn, "person")
    conn.execute("drop table person")
    prevent_data_file_truncation(conn)
    conn.execute("checkpoint")
    free_pages = get_free_page_ranges(conn)
    compare_page_range_lists(used_pages, free_pages)


def test_fsm_reclaim_node_table_delete(fsm_node_table_setup) -> None:
    _, conn = fsm_node_table_setup
    used_pages = get_used_page_ranges(conn, "person")
    conn.execute("match (p:person) delete p")
    prevent_data_file_truncation(conn)
    conn.execute("checkpoint")
    free_pages = get_free_page_ranges(conn)
    compare_page_range_lists(used_pages, free_pages)


def test_fsm_reclaim_rel_table(fsm_rel_table_setup) -> None:
    _, conn = fsm_rel_table_setup
    used_pages = get_used_page_ranges(conn, "knows")
    conn.execute("drop table knows")
    prevent_data_file_truncation(conn)
    conn.execute("checkpoint")
    free_pages = get_free_page_ranges(conn)
    compare_page_range_lists(used_pages, free_pages)


def test_fsm_reclaim_rel_table_delete(fsm_node_table_setup) -> None:
    _, conn = fsm_node_table_setup
    used_pages = get_used_page_ranges(conn, "person")
    conn.execute("match (p:person) delete p")
    prevent_data_file_truncation(conn)
    conn.execute("checkpoint")
    free_pages = get_free_page_ranges(conn)
    compare_page_range_lists(used_pages, free_pages)


def test_fsm_reclaim_struct(fsm_rel_table_setup) -> None:
    _, conn = fsm_rel_table_setup
    used_pages = get_used_page_ranges(conn, "knows", "fwd_summary") + get_used_page_ranges(
        conn, "knows", "bwd_summary"
    )
    conn.execute("alter table knows drop summary")
    prevent_data_file_truncation(conn)
    conn.execute("checkpoint")
    free_pages = get_free_page_ranges(conn)
    compare_page_range_lists(used_pages, free_pages)


def test_fsm_reclaim_rel_group(fsm_rel_group_setup) -> None:
    _, conn = fsm_rel_group_setup
    used_pages = get_used_page_ranges(conn, "likes")
    conn.execute("drop table likes")
    prevent_data_file_truncation(conn)
    conn.execute("checkpoint")
    free_pages = get_free_page_ranges(conn)
    compare_page_range_lists(used_pages, free_pages)


def test_fsm_reclaim_rel_group_column(fsm_rel_group_setup) -> None:
    _, conn = fsm_rel_group_setup
    used_pages = get_used_page_ranges(conn, "likes", "fwd_date") + get_used_page_ranges(conn, "likes", "bwd_date")
    conn.execute("alter table likes drop date")
    prevent_data_file_truncation(conn)
    conn.execute("checkpoint")
    free_pages = get_free_page_ranges(conn)
    compare_page_range_lists(used_pages, free_pages)
