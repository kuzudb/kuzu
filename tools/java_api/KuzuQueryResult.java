package tools.java_api;

public class KuzuQueryResult {
    long qr_ref;
    boolean destoryed = false;

    private void checkNotDestoryed () {
        assert !destoryed: "QueryResult has been destoryed.";
    }

    public void destory () {
        checkNotDestoryed();
        KuzuNative.kuzu_query_result_destroy(this);
        destoryed = true;
    }

    public boolean isSuccess () {
        checkNotDestoryed();
        return KuzuNative.kuzu_query_result_is_success(this);
    }

    public String getErrorMessage () {
        checkNotDestoryed();
        return KuzuNative.kuzu_query_result_get_error_message(this);
    }

    public long getNumColumns () {
        checkNotDestoryed();
        return KuzuNative.kuzu_query_result_get_num_columns(this);
    }

    public String getColumnName (long index) {
        checkNotDestoryed();
        return KuzuNative.kuzu_query_result_get_column_name(this, index);
    }

    public KuzuDataType getColumnDataType (long index) {
        checkNotDestoryed();
        return KuzuNative.kuzu_query_result_get_column_data_type(this, index);
    }

    public long getNumTuples () {
        checkNotDestoryed();
        return KuzuNative.kuzu_query_result_get_num_tuples(this);
    }

    public KuzuQuerySummary getQuerySummary () {
        checkNotDestoryed();
        return KuzuNative.kuzu_query_result_get_query_summary(this);
    }

    public boolean hasNext () {
        checkNotDestoryed();
        return KuzuNative.kuzu_query_result_has_next(this);
    }

    public KuzuFlatTuple getNext () {
        checkNotDestoryed();
        return KuzuNative.kuzu_query_result_get_next(this);
    }

    public String toString () {
        checkNotDestoryed();
        return KuzuNative.kuzu_query_result_to_string(this);
    }

    public void writeToCsv (String file_path, char delimiter, char escape_char, char new_line) {
        checkNotDestoryed();
        KuzuNative.kuzu_query_result_write_to_csv(this, file_path, delimiter, escape_char, new_line);
    }

    public void resetIterator () {
        checkNotDestoryed();
        KuzuNative.kuzu_query_result_reset_iterator(this);
    }
}
