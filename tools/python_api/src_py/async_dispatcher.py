from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .database import Database
from .connection import Connection
from .prepared_statement import PreparedStatement
from .query_result import QueryResult
from concurrent.futures import ThreadPoolExecutor
import asyncio
import threading

if TYPE_CHECKING:
    import sys
    from types import TracebackType
    from .types import Type

    if sys.version_info >= (3, 11):
        from typing import Self
    else:
        from typing_extensions import Self


class AsyncDispatcher:
    """AsyncDispatcher enables asynchronous execution of queries with a pool of connections and threads."""

    """
    Initialise the async dispatcher.

    Parameters
    ----------
    database : Database
        Database to connect to.
    
    max_concurrent_queries : int
        Maximum number of concurrent queries to execute. This corresponds to the 
        number of connections and thread pool size. Default is 4.
    
    max_threads_per_query : int
        Controls the maximum number of threads per connection that can be used 
        to execute one query. Default is 0, which means no limit.
    """
    def __init__(
        self,
        database: Database,
        max_concurrent_queries: int = 4,
        max_threads_per_query: int = 0,
    ) -> None:
        self.database = database
        self.connections = [Connection(database) for _ in range(max_concurrent_queries)]
        self.connections_counter = [0 for _ in range(max_concurrent_queries)]
        self.lock = threading.Lock()

        for conn in self.connections:
            conn.init_connection()
            conn.set_max_threads_for_exec(max_threads_per_query)

        self.executor = ThreadPoolExecutor(max_workers=max_concurrent_queries)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()

    def __get_connection_with_least_queries(self) -> tuple["Connection", int]:
        with self.lock:
            conn_index = self.connections_counter.index(min(self.connections_counter))
            self.connections_counter[conn_index] += 1
        return self.connections[conn_index], conn_index

    def __decrement_connection_counter(self, conn_index: int) -> None:
        """Decrement the query counter for a connection."""
        with self.lock:
            self.connections_counter[conn_index] -= 1

    def acquire_connection(self) -> "Connection":
        """
        Acquire a connection from the connection pool for temporary synchronous 
        calls. If the connection pool is oversubscribed, the method will return 
        the connection with the least number of queued queries. It is required 
        to release the connection by calling `release_connection` after the 
        connection is no longer needed.

        Returns
        -------
        Connection
            A connection object.
        """
        conn, _ = self.__get_connection_with_least_queries()
        return conn

    def release_connection(self, conn) -> None:
        """
        Release a connection acquired by `acquire_connection` back to the
        connection pool. Calling this method is required when the connection is
        no longer needed.

        Parameters
        ----------
        conn : Connection
            Connection object to release.

        Returns
        -------
        QueryResult
            Query result.

        """
        for i, existing_conn in enumerate(self.connections):
            if existing_conn == conn:
                self.__decrement_connection_counter(i)
                break

    def set_query_timeout(self, timeout: int) -> None:
        """
        Set the query timeout value in ms for executing queries.

        Parameters
        ----------
        timeout_in_ms : int
            query timeout value in ms for executing queries.

        """
        for conn in self.connections:
            conn.set_query_timeout(timeout)

    async def execute(
        self, query: str | "PreparedStatement", parameters: dict[str, Any] | None = None
    ) -> "QueryResult" | list["QueryResult"]:
        """
        Execute a query asynchronously.

        Parameters
        ----------
        query : str | PreparedStatement
            A prepared statement or a query string.
            If a query string is given, a prepared statement will be created
            automatically.

        parameters : dict[str, Any]
            Parameters for the query.

        Returns
        -------
        QueryResult
            Query result.

        """
        loop = asyncio.get_running_loop()
        conn, conn_index = self.__get_connection_with_least_queries()

        try:
            return await loop.run_in_executor(
                self.executor, conn.execute, query, parameters
            )
        finally:
            self.__decrement_connection_counter(conn_index)

    async def prepare(self, query: str) -> "PreparedStatement":
        """
        Create a prepared statement for a query asynchronously.

        Parameters
        ----------
        query : str
            Query to prepare.

        Returns
        -------
        PreparedStatement
            Prepared statement.

        """
        loop = asyncio.get_running_loop()
        conn, conn_index = self.__get_connection_with_least_queries()

        try:
            return await loop.run_in_executor(self.executor, conn.prepare, query)
        finally:
            self.__decrement_connection_counter(conn_index)

    def close(self) -> None:
        """
        Close all connections and shutdown the thread pool.
        
        Note: Call to this method is optional. The connections and thread pool
        will be closed automatically when the instance is garbage collected.
        """
        for conn in self.connections:
            conn.close()

        self.executor.shutdown(wait=True)
        