package tools.java_api;

public class KuzuQueryResult {
    long qr_ref;
    boolean destroyed = false;

    private void checkNotDestroyed () throws KuzuObjectRefDestroyedException {
        if (destroyed)
            throw new KuzuObjectRefDestroyedException("KuzuQueryResult has been destroyed.");
    }

    @Override  
    protected void finalize() throws KuzuObjectRefDestroyedException {
        destroy();   
    } 

    public void destroy () throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_query_result_destroy(this);
        destroyed = true;
    }

    public boolean isSuccess () throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_query_result_is_success(this);
    }

    public String getErrorMessage () throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_query_result_get_error_message(this);
    }

    public long getNumColumns () throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_query_result_get_num_columns(this);
    }

    public String getColumnName (long index) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_query_result_get_column_name(this, index);
    }

    public KuzuDataType getColumnDataType (long index) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_query_result_get_column_data_type(this, index);
    }

    public long getNumTuples () throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_query_result_get_num_tuples(this);
    }

    public KuzuQuerySummary getQuerySummary () throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_query_result_get_query_summary(this);
    }

    public boolean hasNext () throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_query_result_has_next(this);
    }

    public KuzuFlatTuple getNext () throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_query_result_get_next(this);
    }

    public String toString () {
        if (destroyed)
            return "KuzuQueryResult has been destroyed.";
        else
            return KuzuNative.kuzu_query_result_to_string(this);
    }

    public void writeToCsv (String file_path, char delimiter, char escape_char, char new_line) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_query_result_write_to_csv(this, file_path, delimiter, escape_char, new_line);
    }

    public void resetIterator () throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_query_result_reset_iterator(this);
    }
}
