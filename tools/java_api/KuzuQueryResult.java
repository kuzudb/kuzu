package tools.java_api;

public class KuzuQueryResult {
    long qr_ref;
    boolean destroyed = false;

    private void checkNotdestroyed () {
        assert !destroyed: "QueryResult has been destoryed.";
    }

    public void destory () {
        checkNotdestroyed();
        KuzuNative.kuzu_query_result_destroy(this);
        destroyed = true;
    }

    public boolean isSuccess () {
        checkNotdestroyed();
        return KuzuNative.kuzu_query_result_is_success(this);
    }

    public String getErrorMessage () {
        checkNotdestroyed();
        return KuzuNative.kuzu_query_result_get_error_message(this);
    }

    public long getNumColumns () {
        checkNotdestroyed();
        return KuzuNative.kuzu_query_result_get_num_columns(this);
    }

    public String getColumnName (long index) {
        checkNotdestroyed();
        return KuzuNative.kuzu_query_result_get_column_name(this, index);
    }

    public KuzuDataType getColumnDataType (long index) {
        checkNotdestroyed();
        return KuzuNative.kuzu_query_result_get_column_data_type(this, index);
    }

    public long getNumTuples () {
        checkNotdestroyed();
        return KuzuNative.kuzu_query_result_get_num_tuples(this);
    }

    public KuzuQuerySummary getQuerySummary () {
        checkNotdestroyed();
        return KuzuNative.kuzu_query_result_get_query_summary(this);
    }

    public boolean hasNext () {
        checkNotdestroyed();
        return KuzuNative.kuzu_query_result_has_next(this);
    }

    public KuzuFlatTuple getNext () {
        checkNotdestroyed();
        return KuzuNative.kuzu_query_result_get_next(this);
    }

    public String toString () {
        checkNotdestroyed();
        return KuzuNative.kuzu_query_result_to_string(this);
    }

    public void writeToCsv (String file_path, char delimiter, char escape_char, char new_line) {
        checkNotdestroyed();
        KuzuNative.kuzu_query_result_write_to_csv(this, file_path, delimiter, escape_char, new_line);
    }

    public void resetIterator () {
        checkNotdestroyed();
        KuzuNative.kuzu_query_result_reset_iterator(this);
    }
}
