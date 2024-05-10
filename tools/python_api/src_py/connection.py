from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

from . import _kuzu
from .prepared_statement import PreparedStatement
from .query_result import QueryResult

if TYPE_CHECKING:
    import sys
    from types import TracebackType

    from .database import Database
    from .types import Type

    if sys.version_info >= (3, 11):
        from typing import Self
    else:
        from typing_extensions import Self


class Connection:
    """Connection to a database."""

    def __init__(self, database: Database, num_threads: int = 0):
        """
        Initialise kuzu database connection.

        Parameters
        ----------
        database : Database
            Database to connect to.

        num_threads : int
            Maximum number of threads to use for executing queries.

        """
        self._connection: Any = None  # (type: _kuzu.Connection from pybind11)
        self.database = database
        self.num_threads = num_threads
        self.is_closed = False
        self.init_connection()

    def __getstate__(self) -> dict[str, Any]:
        state = {
            "database": self.database,
            "num_threads": self.num_threads,
            "_connection": None,
        }
        return state

    def init_connection(self) -> None:
        """Establish a connection to the database, if not already initalised."""
        if self.is_closed:
            error_msg = "Connection is closed."
            raise RuntimeError(error_msg)
        self.database.init_database()
        if self._connection is None:
            self._connection = _kuzu.Connection(self.database._database, self.num_threads)  # type: ignore[union-attr]

    def set_max_threads_for_exec(self, num_threads: int) -> None:
        """
        Set the maximum number of threads for executing queries.

        Parameters
        ----------
        num_threads : int
            Maximum number of threads to use for executing queries.

        """
        self.init_connection()
        self._connection.set_max_threads_for_exec(num_threads)

    def close(self) -> None:
        """
        Close the connection.

        Note: Call to this method is optional. The connection will be closed
        automatically when the object goes out of scope.
        """
        if self._connection is not None:
            self._connection.close()
        self._connection = None
        self.is_closed = True

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def execute(
        self,
        query: str | PreparedStatement,
        parameters: dict[str, Any] | None = None,
    ) -> QueryResult | [QueryResult]:
        """
        Execute a query.

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
        if parameters is None:
            parameters = {}

        self.init_connection()
        if not isinstance(parameters, dict):
            msg = f"Parameters must be a dict; found {type(parameters)}."
            raise RuntimeError(msg)  # noqa: TRY004

        if len(parameters) == 0:
            _query_result = self._connection.query(query)
        else:
            prepared_statement = self.prepare(query) if isinstance(query, str) else query
            _query_result = self._connection.execute(prepared_statement._prepared_statement, parameters)
        if not _query_result.isSuccess():
            raise RuntimeError(_query_result.getErrorMessage())
        current_query_result = QueryResult(self, _query_result)
        if not _query_result.hasNextQueryResult():
            return current_query_result
        all_query_results = [current_query_result]
        while _query_result.hasNextQueryResult():
            _query_result = _query_result.getNextQueryResult()
            if not _query_result.isSuccess():
                raise RuntimeError(_query_result.getErrorMessage())
            all_query_results.append(QueryResult(self, _query_result))
        return all_query_results

    def prepare(self, query: str) -> PreparedStatement:
        """
        Create a prepared statement for a query.

        Parameters
        ----------
        query : str
            Query to prepare.

        Returns
        -------
        PreparedStatement
            Prepared statement.

        """
        return PreparedStatement(self, query)

    def _get_node_property_names(self, table_name: str) -> dict[str, Any]:
        LIST_START_SYMBOL = "["
        LIST_END_SYMBOL = "]"
        self.init_connection()
        query_result = self.execute(f"CALL table_info('{table_name}') RETURN *;")
        results = {}
        while query_result.has_next():
            row = query_result.get_next()
            prop_name = row[1]
            prop_type = row[2]
            is_primary_key = row[3] is True
            dimension = prop_type.count(LIST_START_SYMBOL)
            splitted = prop_type.split(LIST_START_SYMBOL)
            shape = []
            for s in splitted:
                if LIST_END_SYMBOL not in s:
                    continue
                s = s.split(LIST_END_SYMBOL)[0]
                if s != "":
                    shape.append(int(s))
            prop_type = splitted[0]
            results[prop_name] = {
                "type": prop_type,
                "dimension": dimension,
                "is_primary_key": is_primary_key,
            }
            if len(shape) > 0:
                results[prop_name]["shape"] = tuple(shape)
        return results

    def _get_node_table_names(self) -> list[Any]:
        results = []
        self.init_connection()
        query_result = self.execute("CALL show_tables() RETURN *;")
        while query_result.has_next():
            row = query_result.get_next()
            if row[1] == "NODE":
                results.append(row[0])
        return results

    def _get_rel_table_names(self) -> list[dict[str, Any]]:
        results = []
        self.init_connection()
        tables_result = self.execute("CALL show_tables() RETURN *;")
        while tables_result.has_next():
            row = tables_result.get_next()
            if row[1] == "REL":
                name = row[0]
                connections_result = self.execute(f"CALL show_connection({name!r}) RETURN *;")
                src_dst_row = connections_result.get_next()
                src_node = src_dst_row[0]
                dst_node = src_dst_row[1]
                results.append({"name": name, "src": src_node, "dst": dst_node})
        return results

    def set_query_timeout(self, timeout_in_ms: int) -> None:
        """
        Set the query timeout value in ms for executing queries.

        Parameters
        ----------
        timeout_in_ms : int
            query timeout value in ms for executing queries.

        """
        self.init_connection()
        self._connection.set_query_timeout(timeout_in_ms)

    def create_function(
        self,
        name: str,
        udf: Callable[[...], Any],
        params_type: list[Type | str] | None = None,
        return_type: Type | str = "",
    ) -> None:
        """
        Sets a User Defined Function (UDF) to use in cypher queries.

        Parameters
        ----------
        name: str
            name of function

        udf: Callable[[...], Any]
            function to be executed

        params_type: Optional[list[Type]]
            list of Type enums to describe the input parameters

        return_type: Optional[Type]
            a Type enum to describe the returned value
        """
        if params_type is None:
            params_type = []
        parsed_params_type = [x if type(x) is str else x.value for x in params_type]
        if type(return_type) is not str:
            return_type = return_type.value
        self._connection.create_function(name, udf, parsed_params_type, return_type)
