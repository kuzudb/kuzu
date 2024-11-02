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
     * @throws ObjectRefDestroyedException If the prepared statement has been destroyed.
     */
    private void checkNotDestroyed() throws ObjectRefDestroyedException {
        if (destroyed)
            throw new ObjectRefDestroyedException("PreparedStatement has been destroyed.");
    }

    /**
     * Close the prepared statement and release the underlying resources. This method is invoked automatically on objects managed by the try-with-resources statement.
     *
     * @throws ObjectRefDestroyedException If the prepared statement has been destroyed.
     */
    @Override
    public void close() throws ObjectRefDestroyedException {
        destroy();
    }

    /**
     * Destroy the prepared statement.
     *
     * @throws ObjectRefDestroyedException If the prepared statement has been destroyed.
     */
    private void destroy() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        Native.kuzu_prepared_statement_destroy(this);
        destroyed = true;
    }

    /**
     * Check if the query is prepared successfully or not.
     *
     * @return The query is prepared successfully or not.
     * @throws ObjectRefDestroyedException If the prepared statement has been destroyed.
     */
    public boolean isSuccess() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_prepared_statement_is_success(this);
    }

    /**
     * Get the error message if the query is not prepared successfully.
     *
     * @return The error message if the query is not prepared successfully.
     * @throws ObjectRefDestroyedException If the prepared statement has been destroyed.
     */
    public String getErrorMessage() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_prepared_statement_get_error_message(this);
    }

}
