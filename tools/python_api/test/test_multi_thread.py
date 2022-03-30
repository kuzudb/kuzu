import threading

from tools.python_api import _graphflowdb as gdb

NUM_THREAD = 5


def execute_query(conn, results):
    query_result = conn.query("MATCH (a:person) RETURN COUNT(*);")
    if not query_result.hasNext() or not query_result.getNext() == [8]:
        results.append(False)
    else:
        results.append(True)
    query_result.close()


def test_parallel_query_single_connect(establish_connection):
    conn, db = establish_connection

    results = []
    threads = []
    for i in range(0, NUM_THREAD):
        threads.append(threading.Thread(target=execute_query, args=(conn, results), name='thread_' + str(i)))
    for i in range(0, NUM_THREAD):
        threads[i].start()
    for i in range(0, NUM_THREAD):
        if not results[i]:
            assert False
        threads[i].join()


def test_parallel_connect(load_tiny_snb):
    db = gdb.database(load_tiny_snb)
    results = []
    threads = []
    connections = []
    for i in range(0, NUM_THREAD):
        conn = gdb.connection(db)
        connections.append(conn)
        threads.append(threading.Thread(target=execute_query, args=(conn, results), name='thread_' + str(i)))
    for i in range(0, NUM_THREAD):
        threads[i].start()
    for i in range(0, NUM_THREAD):
        if not results[i]:
            assert False
        threads[i].join()
