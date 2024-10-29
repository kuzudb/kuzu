package com.kuzudb;

import java.util.Map;

/**
 * Connection is used to interact with a Database instance. Each Connection is thread-safe. Multiple
 * connections can connect to the same Database instance in a multi-threaded environment.
 */
public class Connection implements AutoCloseable {

    long conn_ref;
    boolean destroyed = false;

    /**
     * Creates a connection to the database.
     *
     * @param db: Database instance.
     */
    public Connection(Database db) {
        if (db == null)
            throw new AssertionError("Cannot create connection, database is null.");
        conn_ref = Native.kuzu_connection_init(db);
    }

    /**
     * Check if the connection has been destroyed.
     *
     * @throws ObjectRefDestroyedException If the connection has been destroyed.
     */
    private void checkNotDestroyed() throws ObjectRefDestroyedException {
        if (destroyed)
            throw new ObjectRefDestroyedException("Connection has been destroyed.");
    }

    /**
     * Close the connection and release the underlying resources. This method is invoked automatically on objects managed by the try-with-resources statement.
     *
     * @throws ObjectRefDestroyedException If the connection has been destroyed.
     */
    @Override
    public void close() throws ObjectRefDestroyedException {
        destroy();
    }

    /**
     * Destroy the connection.
     *
     * @throws ObjectRefDestroyedException If the connection has been destroyed.
     */
    private void destroy() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        Native.kuzu_connection_destroy(this);
        destroyed = true;
    }

    /**
     * Return the maximum number of threads used for execution in the current connection.
     *
     * @return The maximum number of threads used for execution in the current connection.
     * @throws ObjectRefDestroyedException If the connection has been destroyed.
     */
    public long getMaxNumThreadForExec() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_connection_get_max_num_thread_for_exec(this);
    }

    /**
     * Sets the maximum number of threads to use for execution in the current connection.
     *
     * @param numThreads: The maximum number of threads to use for execution in the current connection
     * @throws ObjectRefDestroyedException If the connection has been destroyed.
     */
    public void setMaxNumThreadForExec(long numThreads) throws ObjectRefDestroyedException {
        checkNotDestroyed();
        Native.kuzu_connection_set_max_num_thread_for_exec(this, numThreads);
    }

    /**
     * Executes the given query and returns the result.
     *
     * @param queryStr: The query to execute.
     * @return The result of the query.
     * @throws ObjectRefDestroyedException If the connection has been destroyed.
     */
    public QueryResult query(String queryStr) throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_connection_query(this, queryStr);
    }

    /**
     * Prepares the given query and returns the prepared statement.
     *
     * @param queryStr: The query to prepare.
     * @return The prepared statement.
     * @throws ObjectRefDestroyedException If the connection has been destroyed.
     */
    public PreparedStatement prepare(String queryStr) throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_connection_prepare(this, queryStr);
    }

    /**
     * Executes the given prepared statement with args and returns the result.
     *
     * @param ps: The prepared statement to execute.
     * @param m:  The parameter pack where each arg is a std::pair with the first element being parameter name and second
     *            element being parameter value
     * @return The result of the query.
     * @throws ObjectRefDestroyedException If the connection has been destroyed.
     */
    public QueryResult execute(PreparedStatement ps, Map<String, Value> m) throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_connection_execute(this, ps, m);
    }

    /**
     * Interrupts all queries currently executed within this connection.
     *
     * @throws ObjectRefDestroyedException If the connection has been destroyed.
     */
    public void interrupt() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        Native.kuzu_connection_interrupt(this);
    }

    /**
     * Sets the query timeout value of the current connection. A value of zero (the default) disables the timeout.
     *
     * @param timeoutInMs: The query timeout value in milliseconds.
     * @throws ObjectRefDestroyedException If the connection has been destroyed.
     */
    public void setQueryTimeout(long timeoutInMs) throws ObjectRefDestroyedException {
        checkNotDestroyed();
        Native.kuzu_connection_set_query_timeout(this, timeoutInMs);
    }
}
