import asyncio

import kuzu
import pytest


@pytest.mark.asyncio()
async def test_async_prepare_and_execute(async_connection_readonly):
    query = "MATCH (a:person) WHERE a.ID = $1 RETURN a.age;"
    prepared = await async_connection_readonly.prepare(query)
    result = await async_connection_readonly.execute(prepared, {"1": 0})
    assert result.has_next()
    assert result.get_next() == [35]
    assert not result.has_next()
    result.close()
    for i in async_connection_readonly.connections_counter:
        assert i == 0


@pytest.mark.asyncio()
async def test_async_prepare_and_execute_concurrent(async_connection_readonly):
    num_queries = 100
    query = "RETURN $1;"
    prepared_statements = await asyncio.gather(*[async_connection_readonly.prepare(query) for _ in range(num_queries)])
    results = await asyncio.gather(*[
        async_connection_readonly.execute(prepared, {"1": i}) for i, prepared in enumerate(prepared_statements)
    ])
    for i, result in enumerate(results):
        assert result.has_next()
        assert result.get_next() == [i]
        assert not result.has_next()
        result.close()
    for i in async_connection_readonly.connections_counter:
        assert i == 0


@pytest.mark.asyncio()
async def test_async_query(async_connection_readonly):
    query = "MATCH (a:person) WHERE a.ID = 0 RETURN a.age;"
    result = await async_connection_readonly.execute(query)
    assert result.has_next()
    assert result.get_next() == [35]
    assert not result.has_next()
    result.close()

    query = "MATCH (a:person) WHERE a.ID = 2 RETURN a.age;"
    result = await async_connection_readonly.execute(query)
    assert result.has_next()
    assert result.get_next() == [30]
    assert not result.has_next()
    result.close()
    for i in async_connection_readonly.connections_counter:
        assert i == 0


@pytest.mark.asyncio()
async def test_async_query_concurrent(async_connection_readonly):
    num_queries = 100
    queries = [f"RETURN {i};" for i in range(num_queries)]
    results = await asyncio.gather(*[async_connection_readonly.execute(query) for query in queries])
    for i, result in enumerate(results):
        assert result.has_next()
        assert result.get_next() == [i]
        assert not result.has_next()
        result.close()
    for i in async_connection_readonly.connections_counter:
        assert i == 0


@pytest.mark.asyncio()
async def test_async_query_multiple_results(async_connection_readonly):
    query = "MATCH (a:person) WHERE a.ID = 0 RETURN a.age; MATCH (a:person) WHERE a.ID = 2 RETURN a.age;"
    results = await async_connection_readonly.execute(query)
    assert len(results) == 2
    result = results[0]
    assert result.has_next()
    assert result.get_next() == [35]
    assert not result.has_next()

    result = results[1]
    assert result.has_next()
    assert result.get_next() == [30]
    assert not result.has_next()
    for result in results:
        result.close()
    for i in async_connection_readonly.connections_counter:
        assert i == 0


@pytest.mark.asyncio()
async def test_async_connection_create_and_close():
    db = kuzu.Database(":memory:", buffer_pool_size=2**28)
    async_connection = kuzu.AsyncConnection(db)
    for _ in range(10):
        res = await async_connection.execute("RETURN 1;")
        assert res.has_next()
        assert res.get_next() == [1]
        assert not res.has_next()
        res.close()
    num_queries = 100
    queries = [f"RETURN {i};" for i in range(num_queries)]
    results = await asyncio.gather(*[async_connection.execute(query) for query in queries])
    for i, result in enumerate(results):
        assert result.has_next()
        assert result.get_next() == [i]
        assert not result.has_next()
        result.close()
    async_connection.close()
    db.close()


def test_acquire_connection(async_connection_readonly):
    conn = async_connection_readonly.acquire_connection()
    assert conn is not None
    assert async_connection_readonly.connections_counter[0] == 1
    result = conn.execute("RETURN 1;")
    assert result.has_next()
    assert result.get_next() == [1]
    assert not result.has_next()
    result.close()
    async_connection_readonly.release_connection(conn)
    for i in async_connection_readonly.connections_counter:
        assert i == 0
