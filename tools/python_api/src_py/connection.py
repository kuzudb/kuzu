from .query_result import QueryResult
from .prepared_statement import PreparedStatement
from . import _kuzu


class Connection:
    """
    Connection to a database.
    """

    def __init__(self, database, num_threads=0):
        """
        Parameters
        ----------
        database : Database
            Database to connect to.

        num_threads : int
            Maximum number of threads to use for executing queries.

        """

        self.database = database
        self.num_threads = num_threads
        self._connection = None
        self.init_connection()

    def __getstate__(self):
        state = {
            "database": self.database,
            "num_threads": self.num_threads,
            "_connection": None
        }
        return state

    def init_connection(self):
        self.database.init_database()
        if self._connection is None:
            self._connection = _kuzu.Connection(
                self.database._database, self.num_threads)

    def set_max_threads_for_exec(self, num_threads):
        """
        Set the maximum number of threads for executing queries.

        Parameters
        ----------
        num_threads : int
            Maximum number of threads to use for executing queries.

        """
        self.init_connection()
        self._connection.set_max_threads_for_exec(num_threads)

    def execute(self, query, parameters={}):
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
        self.init_connection()
        if type(parameters) != dict:
            raise RuntimeError("Parameters must be a dict")
        prepared_statement = self.prepare(
            query) if type(query) == str else query
        _query_result = self._connection.execute(
                               prepared_statement._prepared_statement,
                               parameters)
        if not _query_result.isSuccess():
            raise RuntimeError(_query_result.getErrorMessage())
        return QueryResult(self, _query_result)

    def prepare(self, query):
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

    def _get_node_property_names(self, table_name):
        PRIMARY_KEY_SYMBOL = "(PRIMARY KEY)"
        LIST_START_SYMBOL = "["
        LIST_END_SYMBOL = "]"
        self.init_connection()
        query_result = self.execute(
            "CALL table_info('%s') RETURN *;" % table_name)
        results = {}
        while query_result.has_next():
            row = query_result.get_next()
            prop_name = row[1]
            prop_type = row[2]
            is_primary_key = row[3] == True
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
                "is_primary_key": is_primary_key
            }
            if len(shape) > 0:
                results[prop_name]["shape"] = tuple(shape)
        return results

    def _get_node_table_names(self):
        results = []
        self.init_connection()
        query_result = self.execute("CALL show_tables() RETURN *;")
        while query_result.has_next():
            row = query_result.get_next()
            if row[1] == "NODE":
                results.append(row[0])
        return results

    def _get_rel_table_names(self):
        results = []
        self.init_connection()
        tables_result = self.execute("CALL show_tables() RETURN *;")
        while tables_result.has_next():
            row = tables_result.get_next()
            if row[1] == "REL":
                name = row[0]
                connections_result = self.execute("CALL show_connection('%s') RETURN *;" % name)
                src_dst_row = connections_result.get_next()
                src_node = src_dst_row[0]
                dst_node = src_dst_row[1]
                results.append({
                    "name": name,
                    "src": src_node,
                    "dst": dst_node
                })
        return results

    def set_query_timeout(self, timeout_in_ms):
        """
        Set the query timeout value in ms for executing queries.

        Parameters
        ----------
        timeout_in_ms : int
            query timeout value in ms for executing queries.
            
        """
        self.init_connection()
        self._connection.set_query_timeout(timeout_in_ms)
