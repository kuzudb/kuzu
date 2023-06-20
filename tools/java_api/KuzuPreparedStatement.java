package tools.java_api;

public class KuzuPreparedStatement {
    long ps_ref;
    boolean destroyed = false;

    private void checkNotdestroyed() {
        assert !destroyed: "PreparedStatement has been destoryed.";
    }

    public void destory() {
        checkNotdestroyed();
        KuzuNative.kuzu_prepared_statement_destroy(this);
        destroyed = true;
    }

    public boolean allowActiveTransaction() {
        checkNotdestroyed();
        return KuzuNative.kuzu_prepared_statement_allow_active_transaction(this);
    }

    public boolean isSuccess() {
        checkNotdestroyed();
        return KuzuNative.kuzu_prepared_statement_is_success(this);
    }

    public String getErrorMessage() {
        checkNotdestroyed();
        return KuzuNative.kuzu_prepared_statement_get_error_message(this);
    }

}
