package com.kuzudb;

import java.util.Map;

/**
 * KuzuConnection is used to interact with a KuzuDatabase instance. Each KuzuConnection is thread-safe. Multiple
 * connections can connect to the same KuzuDatabase instance in a multi-threaded environment.
 */
public class KuzuConnection {

    long conn_ref;
    boolean destroyed = false;

    /**
    * Creates a connection to the database.
    * @param db: KuzuDatabase instance.
    */
    public KuzuConnection(KuzuDatabase db) {
        assert db != null : "Cannot create connection, database is null.";
        conn_ref = KuzuNative.kuzu_connection_init(db);
    }

    /**
    * Check if the connection has been destroyed.
    * @throws KuzuObjectRefDestroyedException If the connection has been destroyed.
    */
    private void checkNotDestroyed() throws KuzuObjectRefDestroyedException {
        if (destroyed)
            throw new KuzuObjectRefDestroyedException("KuzuConnection has been destroyed.");
    }

    /**
    * Finalize.
    * @throws KuzuObjectRefDestroyedException If the connection has been destroyed.
    */
    @Override
    protected void finalize() throws KuzuObjectRefDestroyedException {
        destroy();
    }

    /**
    * Destroy the connection.
    * @throws KuzuObjectRefDestroyedException If the connection has been destroyed.
    */
    public void destroy() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_connection_destroy(this);
        destroyed = true;
    }

    /**
    * Manually starts a new read-only transaction in the current connection.
    * @throws KuzuObjectRefDestroyedException If the connection has been destroyed.
    */
    public void beginReadOnlyTransaction() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_connection_begin_read_only_transaction(this);
    }

    /**
    * Manually starts a new write transaction in the current connection.
    * @throws KuzuObjectRefDestroyedException If the connection has been destroyed.
    */
    public void beginWriteTransaction() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_connection_begin_write_transaction(this);
    }

    /**
    * Manually commits the current transaction.
    * @throws KuzuObjectRefDestroyedException If the connection has been destroyed.
    */
    public void commit() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_connection_commit(this);
    }

    /**
    * Manually rolls back the current transaction.
    * @throws KuzuObjectRefDestroyedException If the connection has been destroyed.
    */
    public void rollback() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_connection_rollback(this);
    }

    /**
    * Return the maximum number of threads used for execution in the current connection.
    * @return The maximum number of threads used for execution in the current connection.
    * @throws KuzuObjectRefDestroyedException If the connection has been destroyed.
    */
    public long getMaxNumThreadForExec() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_connection_get_max_num_thread_for_exec(this);
    }

    /**
    * Sets the maximum number of threads to use for execution in the current connection.
    * @param numThreads: The maximum number of threads to use for execution in the current connection
    * @throws KuzuObjectRefDestroyedException If the connection has been destroyed.
    */
    public void setMaxNumThreadForExec(long numThreads) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_connection_set_max_num_thread_for_exec(this, numThreads);
    }

    /**
    * Executes the given query and returns the result.
    * @param queryStr: The query to execute.
    * @return The result of the query.
    * @throws KuzuObjectRefDestroyedException If the connection has been destroyed.
    */
    public KuzuQueryResult query(String queryStr) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_connection_query(this, queryStr);
    }

    /**
    * Prepares the given query and returns the prepared statement.
    * @param queryStr: The query to prepare.
    * @return The prepared statement.
    * @throws KuzuObjectRefDestroyedException If the connection has been destroyed.
    */
    public KuzuPreparedStatement prepare(String queryStr) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_connection_prepare(this, queryStr);
    }

    /**
    * Executes the given prepared statement with args and returns the result.
    * @param ps: The prepared statement to execute.
    * @param m: The parameter pack where each arg is a std::pair with the first element being parameter name and second
    * element being parameter value
    * @return The result of the query.
    * @throws KuzuObjectRefDestroyedException If the connection has been destroyed.
    */
    public KuzuQueryResult execute(KuzuPreparedStatement ps, Map<String, KuzuValue> m) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_connection_execute(this, ps, m);
    }

    /**
    * Interrupts all queries currently executed within this connection.
    * @throws KuzuObjectRefDestroyedException If the connection has been destroyed.
    */
    public void interrupt() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_connection_interrupt(this);
    }

    /**
    * Sets the query timeout value of the current connection. A value of zero (the default) disables the timeout.
    * @param timeoutInMs: The query timeout value in milliseconds.
    * @throws KuzuObjectRefDestroyedException If the connection has been destroyed.
    */
    public void setQueryTimeout(long timeoutInMs) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_connection_set_query_timeout(this, timeoutInMs);
    }
}
