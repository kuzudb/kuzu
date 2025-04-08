import random
from pathlib import Path

import kuzu
from type_aliases import ConnDB


def get_num_used_pages(conn):
    result = conn.execute("call show_tables() return name").get_as_df()
    chunk_sizes = []
    # we use the max page idx as another approximation of the database size
    max_page_idx = 0
    for table in result["name"]:
        storage_info = conn.execute(f'call storage_info("{table}") return start_page_idx, num_pages')
        while storage_info.has_next():
            cur_tuple = storage_info.get_next()
            if cur_tuple[1] > 0:
                chunk_sizes.append(cur_tuple[1])
                max_page_idx = max(max_page_idx, cur_tuple[0] + cur_tuple[1])
    return sum(chunk_sizes), max_page_idx


def get_num_free_pages(conn):
    num_free_pages = []
    result = conn.execute("call fsm_info() return *")
    max_page_idx = 0
    while result.has_next():
        cur_tuple = result.get_next()
        if cur_tuple[1] > 0:
            num_free_pages.append(cur_tuple[1])
            max_page_idx = max(max_page_idx, cur_tuple[0] + cur_tuple[1])
    return sum(num_free_pages), max_page_idx


def get_total_num_pages(conn):
    num_used_pages, max_used_page_idx = get_num_used_pages(conn)
    num_free_pages, max_free_page_idx = get_num_free_pages(conn)
    return num_used_pages, num_free_pages, max(max_used_page_idx, max_free_page_idx)


def create_schema(conn):
    conn.execute("create node table values(id serial, value uint64, primary key (id))")


def test_fsm_decrease_bitwidths(tmp_path: Path) -> None:
    with kuzu.Database(tmp_path) as db, kuzu.Connection(db) as conn:
        create_schema(conn)
        conn.execute("call auto_checkpoint=false")

        # create data
        num_tuples = 2**17
        vals = [0 if i % 2 == 0 else 2**16 - 1 for i in range(num_tuples)]
        prep = conn.prepare("unwind $vals as i create(:values{value: i})")
        conn.execute(prep, {"vals": vals})
        conn.execute("checkpoint")

        _, _, start_num_pages = get_total_num_pages(conn)

        # update vals
        num_updates = 10
        update_batch_size = 10
        prep = conn.prepare("match (p:values) where p.id = $id set p.value = $val")
        for _ in range(num_updates):
            for i in range(update_batch_size):
                id = random.randint(0, num_tuples - 1)
                new_val = 0 if i % 2 == 0 else 2**8 - 1
                conn.execute(prep, {"id": id, "val": new_val})
            conn.execute("checkpoint")

        _, _, end_num_pages = get_total_num_pages(conn)
        assert end_num_pages <= start_num_pages


def test_fsm_incremental_copy(tmp_path: Path):
    with kuzu.Database(tmp_path) as db, kuzu.Connection(db) as conn:
        create_schema(conn)
        conn.execute("call auto_checkpoint=false")

        num_tuples = 100000
        batch_size = 1000
        vals = [random.randint(0, 2**16 - 1) for _ in range(num_tuples)]
        prep = conn.prepare("copy values from (unwind $vals as i return CAST(i as UINT64))")
        for i in range(0, num_tuples, batch_size):
            conn.execute(
                prep,
                {"vals": vals[i : i + batch_size]},
            )

        num_used_pages, _, num_total_pages = get_total_num_pages(conn)
        assert num_used_pages / num_total_pages >= 0.33
