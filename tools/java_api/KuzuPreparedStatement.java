package tools.java_api;

public class KuzuPreparedStatement {
    long ps_ref;
    boolean destoryed = false;

    private void checkNotDestoryed() {
        assert !destoryed: "PreparedStatement has been destoryed.";
    }

    public void destoryPreparedStatement() {
        checkNotDestoryed();
        KuzuNative.kuzu_prepared_statement_destroy(this);
        destoryed = true;
    }

    public boolean allowActiveTransaction() {
        checkNotDestoryed();
        return KuzuNative.kuzu_prepared_statement_allow_active_transaction(this);
    }

    public boolean isSuccess() {
        checkNotDestoryed();
        return KuzuNative.kuzu_prepared_statement_is_success(this);
    }

    public String getErrorMessage() {
        checkNotDestoryed();
        return KuzuNative.kuzu_prepared_statement_get_error_message(this);
    }

    public void bindBool(String param_name, boolean value) {
        checkNotDestoryed();
        KuzuNative.kuzu_prepared_statement_bind_bool(this, param_name, value);
    }

    public void bindInt64(String param_name, long value) {
        checkNotDestoryed();
        KuzuNative.kuzu_prepared_statement_bind_int64(this, param_name, value);
    }

    public void bindInt32(String param_name, int value) {
        checkNotDestoryed();
        KuzuNative.kuzu_prepared_statement_bind_int32(this, param_name, value);
    }

    public void bindInt16(String param_name, short value)  {
        checkNotDestoryed();
        KuzuNative.kuzu_prepared_statement_bind_int16(this, param_name, value);
    }

    public void bindDouble(String param_name, double value)  {
        checkNotDestoryed();
        KuzuNative.kuzu_prepared_statement_bind_double(this, param_name, value);
    }

    public void bindFloat(String param_name, float value)  {
        checkNotDestoryed();
        KuzuNative.kuzu_prepared_statement_bind_float(this, param_name, value);
    }

    public void bindDate(String param_name, KuzuDate value)  {
        checkNotDestoryed();
        KuzuNative.kuzu_prepared_statement_bind_date(this, param_name, value);
    }

    public void bindTimestamp(String param_name, KuzuTimestamp value)  {
        checkNotDestoryed();
        KuzuNative.kuzu_prepared_statement_bind_timestamp(this, param_name, value);
    }

    public void bindInterval(String param_name, KuzuInterval value)  {
        checkNotDestoryed();
        KuzuNative.kuzu_prepared_statement_bind_interval(this, param_name, value);
    }

    public void bindString(String param_name, String value)  {
        checkNotDestoryed();
        KuzuNative.kuzu_prepared_statement_bind_string(this, param_name, value);
    }

    public void bindValue(String param_name, KuzuValue value)  {
        checkNotDestoryed();
        KuzuNative.kuzu_prepared_statement_bind_value(this, param_name, value);
    }

}
