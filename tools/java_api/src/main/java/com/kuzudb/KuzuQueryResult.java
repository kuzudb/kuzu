package com.kuzudb;

/**
 * KuzuQueryResult stores the result of a query execution.
 */
public class KuzuQueryResult {
    long qr_ref;
    boolean destroyed = false;

    /**
     * Check if the query result has been destroyed.
     * @throws KuzuObjectRefDestroyedException If the query result has been destroyed.
     */
    private void checkNotDestroyed() throws KuzuObjectRefDestroyedException {
        if (destroyed)
            throw new KuzuObjectRefDestroyedException("KuzuQueryResult has been destroyed.");
    }

    /**
     * Finalize.
     * @throws KuzuObjectRefDestroyedException If the query result has been destroyed.
     */
    @Override
    protected void finalize() throws KuzuObjectRefDestroyedException {
        destroy();
    }

    /**
     * Destroy the query result.
     * @throws KuzuObjectRefDestroyedException If the query result has been destroyed.
     */
    public void destroy() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_query_result_destroy(this);
        destroyed = true;
    }

    /**
     * Check if the query is executed successfully.
     * @return Query is executed successfully or not.
     * @throws KuzuObjectRefDestroyedException If the query result has been destroyed.
     */
    public boolean isSuccess() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_query_result_is_success(this);
    }

    /**
     * Get the error message if any.
     * @return Error message of the query execution if the query fails.
     * @throws KuzuObjectRefDestroyedException If the query result has been destroyed.
     */
    public String getErrorMessage() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_query_result_get_error_message(this);
    }

    /**
     * Get the number of columns in the query result.
     * @return The number of columns in the query result.
     * @throws KuzuObjectRefDestroyedException If the query result has been destroyed.
     */
    public long getNumColumns() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_query_result_get_num_columns(this);
    }

    /**
     * Get the column name at the given index.
     * @param index: The index of the column.
     * @return The column name at the given index.
     * @throws KuzuObjectRefDestroyedException If the query result has been destroyed.
     */
    public String getColumnName(long index) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_query_result_get_column_name(this, index);
    }

    /**
     * Get the column data type at the given index.
     * @param index: The index of the column.
     * @return The column data type at the given index.
     * @throws KuzuObjectRefDestroyedException If the query result has been destroyed.
     */
    public KuzuDataType getColumnDataType(long index) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_query_result_get_column_data_type(this, index);
    }

    /**
     * Get the number of tuples in the query result.
     * @return The number of tuples in the query result.
     * @throws KuzuObjectRefDestroyedException If the query result has been destroyed.
     */
    public long getNumTuples() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_query_result_get_num_tuples(this);
    }

    /**
     * Get the query summary.
     * @return The query summary.
     * @throws KuzuObjectRefDestroyedException If the query result has been destroyed.
     */
    public KuzuQuerySummary getQuerySummary() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_query_result_get_query_summary(this);
    }

    /**
     * Return if the query result has next tuple or not.
     * @return Whether there are more tuples to read.
     * @throws KuzuObjectRefDestroyedException If the query result has been destroyed.
     */
    public boolean hasNext() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_query_result_has_next(this);
    }

    /**
     * Get the next tuple.
     * @return The next tuple.
     * @throws KuzuObjectRefDestroyedException If the query result has been destroyed.
     */
    public KuzuFlatTuple getNext() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_query_result_get_next(this);
    }

    /**
     * Convert the query result to string.
     * @return The string representation of the query result.
     */
    public String toString() {
        if (destroyed)
            return "KuzuQueryResult has been destroyed.";
        else
            return KuzuNative.kuzu_query_result_to_string(this);
    }

    /**
     * Write the query result to CSV file.
     * @param filePath: The path of the CSV file.
     * @param delimiter: The delimiter of the CSV file.
     * @param escapeChar: The escape character of the CSV file.
     * @param newLine: The new line character of the CSV file.
     * @throws KuzuObjectRefDestroyedException If the query result has been destroyed.
     *
     */
    public void writeToCsv(String filePath, char delimiter, char escapeChar, char newLine) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_query_result_write_to_csv(this, filePath, delimiter, escapeChar, newLine);
    }

    /**
     * Reset the query result iterator.
     * @throws KuzuObjectRefDestroyedException If the query result has been destroyed.
     */
    public void resetIterator() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_query_result_reset_iterator(this);
    }
}
