from .query_result import QueryResult
from . import _kuzu


class Connection:
    def __init__(self, database, num_threads=0):
        self.database = database
        self._connection = _kuzu.Connection(database, num_threads)

    def set_max_threads_for_exec(self, num_threads):
        self._connection.set_max_threads_for_exec(num_threads)

    def execute(self, query, parameters=[]):
        return QueryResult(self, self._connection.execute(query, parameters))
