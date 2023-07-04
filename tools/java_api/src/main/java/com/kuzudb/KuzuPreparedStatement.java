package com.kuzudb;

public class KuzuPreparedStatement {
    long ps_ref;
    boolean destroyed = false;

    private void checkNotDestroyed() throws KuzuObjectRefDestroyedException {
        if (destroyed)
            throw new KuzuObjectRefDestroyedException("KuzuPreparedStatement has been destroyed.");
    }

    @Override
    protected void finalize() throws KuzuObjectRefDestroyedException {
        destroy();
    }

    public void destroy() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_prepared_statement_destroy(this);
        destroyed = true;
    }

    public boolean allowActiveTransaction() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_prepared_statement_allow_active_transaction(this);
    }

    public boolean isSuccess() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_prepared_statement_is_success(this);
    }

    public String getErrorMessage() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_prepared_statement_get_error_message(this);
    }

}
