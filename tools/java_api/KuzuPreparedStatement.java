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
/*
    public void bindBool(String param_name, boolean value) {
        checkNotdestroyed();
        KuzuNative.kuzu_prepared_statement_bind_bool(this, param_name, value);
    }

    public void bindInt64(String param_name, long value) {
        checkNotdestroyed();
        KuzuNative.kuzu_prepared_statement_bind_int64(this, param_name, value);
    }

    public void bindInt32(String param_name, int value) {
        checkNotdestroyed();
        KuzuNative.kuzu_prepared_statement_bind_int32(this, param_name, value);
    }

    public void bindInt16(String param_name, short value)  {
        checkNotdestroyed();
        KuzuNative.kuzu_prepared_statement_bind_int16(this, param_name, value);
    }

    public void bindDouble(String param_name, double value)  {
        checkNotdestroyed();
        KuzuNative.kuzu_prepared_statement_bind_double(this, param_name, value);
    }

    public void bindFloat(String param_name, float value)  {
        checkNotdestroyed();
        KuzuNative.kuzu_prepared_statement_bind_float(this, param_name, value);
    }

    public void bindDate(String param_name, KuzuDate value)  {
        checkNotdestroyed();
        KuzuNative.kuzu_prepared_statement_bind_date(this, param_name, value);
    }

    public void bindTimestamp(String param_name, KuzuTimestamp value)  {
        checkNotdestroyed();
        KuzuNative.kuzu_prepared_statement_bind_timestamp(this, param_name, value);
    }

    public void bindInterval(String param_name, KuzuInterval value)  {
        checkNotdestroyed();
        KuzuNative.kuzu_prepared_statement_bind_interval(this, param_name, value);
    }

    public void bindString(String param_name, String value)  {
        checkNotdestroyed();
        KuzuNative.kuzu_prepared_statement_bind_string(this, param_name, value);
    }

    public void bindValue(String param_name, KuzuValue value)  {
        checkNotdestroyed();
        KuzuNative.kuzu_prepared_statement_bind_value(this, param_name, value);
    }
 */

}
