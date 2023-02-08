from .query_result import QueryResult
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
    """
    
    def __init__(self, database, num_threads=0):
        """
        Parameters
        ----------
        database : _kuzu.Database
            Database to connect to.
        num_threads : int
            Maximum number of threads to use for executing queries.
        """

        self.database = database
        self._connection = _kuzu.Connection(database, num_threads)

    def set_max_threads_for_exec(self, num_threads):
        """
        Set the maximum number of threads for executing queries.

        Parameters
        ----------
        num_threads : int
            Maximum number of threads to use for executing queries.
        """

        self._connection.set_max_threads_for_exec(num_threads)

    def execute(self, query, parameters=[]):
        """
        Execute a query.

        Parameters
        ----------
        query : str
            Query to execute.
        parameters : list
            Parameters for the query.

        Returns
        -------
        QueryResult
            Query result.
        """

        return QueryResult(self, self._connection.execute(query, parameters))

    def _get_node_property_names(self, table_name):
        PRIMARY_KEY_SYMBOL = "(PRIMARY KEY)"
        LIST_SYMBOL = "[]"
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
            dimension = prop_type.count(LIST_SYMBOL)
            prop_type = prop_type.replace(LIST_SYMBOL, "")
            results[prop_name] = {
                "type": prop_type,
                "dimension": dimension,
                "is_primary_key": is_primary_key
            }
        return results
