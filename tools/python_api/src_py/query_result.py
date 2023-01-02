class QueryResult:
    def __init__(self, connection, query_result):
        self.connection = connection
        self._query_result = query_result
        self.is_closed = False

    def __del__(self):
        self.close()

    def check_for_query_result_close(self):
        if self.is_closed:
            raise Exception("Query result is closed")

    def has_next(self):
        self.check_for_query_result_close()
        return self._query_result.hasNext()

    def get_next(self):
        self.check_for_query_result_close()
        return self._query_result.getNext()

    def write_to_csv(self, filename, delimiter=',', escapeCharacter='"', newline='\n'):
        self.check_for_query_result_close()
        self._query_result.writeToCSV(
            filename, delimiter, escapeCharacter, newline)

    def close(self):
        if self.is_closed:
            return
        self._query_result.close()
        # Allows the connection to be garbage collected if the query result
        # is closed manually by the user.
        self.connection = None
        self.is_closed = True

    def get_as_df(self):
        self.check_for_query_result_close()
        return self._query_result.getAsDF()

    def get_column_data_types(self):
        self.check_for_query_result_close()
        return self._query_result.getColumnDataTypes()

    def get_column_names(self):
        self.check_for_query_result_close()
        return self._query_result.getColumnNames()
