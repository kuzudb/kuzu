from .query_result import QueryResult
from .prepared_statement import PreparedStatement
from . import _kuzu


class Connection:
    """
    Connection to a database.

    Methods
    -------
    set_max_threads_for_exec(num_threads)
        Set the maximum number of threads for executing queries.

    execute(query, parameters=[])
        Execute a query.

    prepare(query)
        Create a prepared statement for a query.

    set_query_timeout(timeout_in_ms)
        Set the query timeout in milliseconds.
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

    def execute(self, query, parameters=[]):
        """
        Execute a query.

        Parameters
        ----------
        query : str | PreparedStatement
            A prepared statement or a query string.
            If a query string is given, a prepared statement will be created
            automatically.
        parameters : list[tuple(str, any)]
            Parameters for the query.

        Returns
        -------
        QueryResult
            Query result.
        """
        self.init_connection()
        prepared_statement = self.prepare(
            query) if type(query) == str else query
        return QueryResult(self,
                           self._connection.execute(
                               prepared_statement._prepared_statement,
                               parameters)
                           )

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
        result_str = self._connection.get_node_property_names(
            table_name)
        results = {}
        for (i, line) in enumerate(result_str.splitlines()):
            # ignore first line
            if i == 0:
                continue
            line = line.strip()
            if line == "":
                continue
            line_splited = line.split(" ")
            if len(line_splited) < 2:
                continue

            prop_name = line_splited[0]
            prop_type = " ".join(line_splited[1:])

            is_primary_key = PRIMARY_KEY_SYMBOL in prop_type
            prop_type = prop_type.replace(PRIMARY_KEY_SYMBOL, "")
            dimension = prop_type.count(LIST_START_SYMBOL)
            splited = prop_type.split(LIST_START_SYMBOL)
            shape = []
            for s in splited:
                if LIST_END_SYMBOL not in s:
                    continue
                s = s.split(LIST_END_SYMBOL)[0]
                if s != "":
                    shape.append(int(s))
            prop_type = splited[0]
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
        result_str = self._connection.get_node_table_names()
        for (i, line) in enumerate(result_str.splitlines()):
            # ignore first line
            if i == 0:
                continue
            line = line.strip()
            if line == "":
                continue
            results.append(line)
        return results

    def _get_rel_table_names(self):
        results = []
        self.init_connection()
        result_str = self._connection.get_rel_table_names()
        for i, line in enumerate(result_str.splitlines()):
            if i == 0:
                continue
            line = line.strip()
            if line == "":
                continue
            props_str = self._connection.get_rel_property_names(line)
            src_node = None
            dst_node = None
            for prop_str in props_str.splitlines():
                if "src node:" in prop_str:
                    src_node = prop_str.split(":")[1].strip()
                if "dst node:" in prop_str:
                    dst_node = prop_str.split(":")[1].strip()
                if src_node is not None and dst_node is not None:
                    break
            results.append({"name": line, "src": src_node, "dst": dst_node})
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
