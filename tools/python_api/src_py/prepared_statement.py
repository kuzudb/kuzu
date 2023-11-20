class PreparedStatement:
    """
    A prepared statement is a parameterized query which can avoid planning the 
    same query for repeated execution.
    """

    def __init__(self, connection, query):
        """
        Parameters
        ----------
        connection : Connection
            Connection to a database.
        query : str
            Query to prepare.
        """

        self._prepared_statement = connection._connection.prepare(query)

    def is_success(self):
        """
        Check if the prepared statement is successfully prepared.

        Returns
        -------
        bool
            True if the prepared statement is successfully prepared.
        """

        return self._prepared_statement.is_success()

    def get_error_message(self):
        """
        Get the error message if the query is not prepared successfully.

        Returns
        -------
        str
            Error message.
        """

        return self._prepared_statement.get_error_message()
