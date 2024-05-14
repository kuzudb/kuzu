package com.kuzudb;

/**
 * KuzuPreparedStatement is a parameterized query which can avoid planning the same query for repeated execution.
 */
public class KuzuPreparedStatement {
    long ps_ref;
    boolean destroyed = false;

    /**
     * Check if the prepared statement has been destroyed.
     * @throws KuzuObjectRefDestroyedException If the prepared statement has been destroyed.
     */
    private void checkNotDestroyed() throws KuzuObjectRefDestroyedException {
        if (destroyed)
            throw new KuzuObjectRefDestroyedException("KuzuPreparedStatement has been destroyed.");
    }

    /**
    * Finalize.
    * @throws KuzuObjectRefDestroyedException If the prepared statement has been destroyed.
    */
    @Override
    protected void finalize() throws KuzuObjectRefDestroyedException {
        destroy();
    }

    /**
     * Destroy the prepared statement.
     * @throws KuzuObjectRefDestroyedException If the prepared statement has been destroyed.
     */
    public void destroy() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_prepared_statement_destroy(this);
        destroyed = true;
    }

    /**
     * Check if the query is prepared successfully or not.
     * @return The query is prepared successfully or not.
     * @throws KuzuObjectRefDestroyedException If the prepared statement has been destroyed.
     */
    public boolean isSuccess() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_prepared_statement_is_success(this);
    }

    /**
     * Get the error message if the query is not prepared successfully.
     * @return The error message if the query is not prepared successfully.
     * @throws KuzuObjectRefDestroyedException If the prepared statement has been destroyed.
     */
    public String getErrorMessage() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_prepared_statement_get_error_message(this);
    }

}
