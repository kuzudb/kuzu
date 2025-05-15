package com.kuzudb;

/**
 * PreparedStatement is a parameterized query which can avoid planning the same query for repeated execution.
 */
public class PreparedStatement implements AutoCloseable {
    long ps_ref;
    boolean destroyed = false;

    /**
     * Check if the prepared statement has been destroyed.
     *
     * @throws RuntimeException If the prepared statement has been destroyed.
     */
    private void checkNotDestroyed() throws RuntimeException {
        if (destroyed)
            throw new RuntimeException("PreparedStatement has been destroyed.");
    }

    /**
     * Close the prepared statement and release the underlying resources. This method is invoked automatically on objects managed by the try-with-resources statement.
     *
     * @throws RuntimeException If the prepared statement has been destroyed.
     */
    @Override
    public void close() throws RuntimeException {
        destroy();
    }

    /**
     * Destroy the prepared statement.
     *
     * @throws RuntimeException If the prepared statement has been destroyed.
     */
    private void destroy() throws RuntimeException {
        checkNotDestroyed();
        Native.kuzu_prepared_statement_destroy(this);
        destroyed = true;
    }

    /**
     * Check if the query is prepared successfully or not.
     *
     * @return The query is prepared successfully or not.
     * @throws RuntimeException If the prepared statement has been destroyed.
     */
    public boolean isSuccess() throws RuntimeException {
        checkNotDestroyed();
        return Native.kuzu_prepared_statement_is_success(this);
    }

    /**
     * Get the error message if the query is not prepared successfully.
     *
     * @return The error message if the query is not prepared successfully.
     * @throws RuntimeException If the prepared statement has been destroyed.
     */
    public String getErrorMessage() throws RuntimeException {
        checkNotDestroyed();
        return Native.kuzu_prepared_statement_get_error_message(this);
    }

}
