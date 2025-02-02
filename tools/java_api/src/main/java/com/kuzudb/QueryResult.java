package com.kuzudb;

/**
 * QueryResult stores the result of a query execution.
 */
public class QueryResult implements AutoCloseable {
    long qr_ref;
    boolean destroyed = false;
    boolean isOwnedByCPP = false;

    /**
     * Check if the query result has been destroyed.
     *
     * @throws ObjectRefDestroyedException If the query result has been destroyed.
     */
    private void checkNotDestroyed() throws ObjectRefDestroyedException {
        if (destroyed)
            throw new ObjectRefDestroyedException("QueryResult has been destroyed.");
    }

    /**
     * Close the query result and release the underlying resources. This method is invoked automatically on objects managed by the try-with-resources statement.
     *
     * @throws ObjectRefDestroyedException If the query result has been destroyed.
     */
    @Override
    public void close() throws ObjectRefDestroyedException {
        destroy();
    }

    public boolean isOwnedByCPP() {
        return isOwnedByCPP;
    }

    /**
     * Destroy the query result.
     *
     * @throws ObjectRefDestroyedException If the query result has been destroyed.
     */
    private void destroy() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        if (!isOwnedByCPP) {
            Native.kuzu_query_result_destroy(this);
            destroyed = true;
        }
    }

    /**
     * Check if the query is executed successfully.
     *
     * @return Query is executed successfully or not.
     * @throws ObjectRefDestroyedException If the query result has been destroyed.
     */
    public boolean isSuccess() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_query_result_is_success(this);
    }

    /**
     * Get the error message if any.
     *
     * @return Error message of the query execution if the query fails.
     * @throws ObjectRefDestroyedException If the query result has been destroyed.
     */
    public String getErrorMessage() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_query_result_get_error_message(this);
    }

    /**
     * Get the number of columns in the query result.
     *
     * @return The number of columns in the query result.
     * @throws ObjectRefDestroyedException If the query result has been destroyed.
     */
    public long getNumColumns() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_query_result_get_num_columns(this);
    }

    /**
     * Get the column name at the given index.
     *
     * @param index: The index of the column.
     * @return The column name at the given index.
     * @throws ObjectRefDestroyedException If the query result has been destroyed.
     */
    public String getColumnName(long index) throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_query_result_get_column_name(this, index);
    }

    /**
     * Get the column data type at the given index.
     *
     * @param index: The index of the column.
     * @return The column data type at the given index.
     * @throws ObjectRefDestroyedException If the query result has been destroyed.
     */
    public DataType getColumnDataType(long index) throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_query_result_get_column_data_type(this, index);
    }

    /**
     * Get the number of tuples in the query result.
     *
     * @return The number of tuples in the query result.
     * @throws ObjectRefDestroyedException If the query result has been destroyed.
     */
    public long getNumTuples() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_query_result_get_num_tuples(this);
    }

    /**
     * Get the query summary.
     *
     * @return The query summary.
     * @throws ObjectRefDestroyedException If the query result has been destroyed.
     */
    public QuerySummary getQuerySummary() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_query_result_get_query_summary(this);
    }

    /**
     * Return if the query result has next tuple or not.
     *
     * @return Whether there are more tuples to read.
     * @throws ObjectRefDestroyedException If the query result has been destroyed.
     */
    public boolean hasNext() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_query_result_has_next(this);
    }

    /**
     * Get the next tuple. Note that to reduce resource allocation, all calls to
     * getNext() reuse the same FlatTuple object. Since its contents will be
     * overwritten, please complete processing a FlatTuple or make a copy of its
     * data before calling getNext() again.
     *
     * @return The next tuple.
     * @throws ObjectRefDestroyedException If the query result has been destroyed.
     */
    public FlatTuple getNext() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_query_result_get_next(this);
    }

    /**
     * Return if the query result has next query result or not.
     *
     * @return Whether there are more query results to read.
     * @throws ObjectRefDestroyedException If the query result has been destroyed.
     */
    public boolean hasNextQueryResult() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_query_result_has_next_query_result(this);
    }

    /**
     * Get the next query result.
     *
     * @return The next query result.
     * @throws ObjectRefDestroyedException If the query result has been destroyed.
     */
    public QueryResult getNextQueryResult() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_query_result_get_next_query_result(this);
    }

    /**
     * Convert the query result to string.
     *
     * @return The string representation of the query result.
     */
    public String toString() {
        if (destroyed)
            return "QueryResult has been destroyed.";
        else
            return Native.kuzu_query_result_to_string(this);
    }

    /**
     * Reset the query result iterator.
     *
     * @throws ObjectRefDestroyedException If the query result has been destroyed.
     */
    public void resetIterator() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        Native.kuzu_query_result_reset_iterator(this);
    }
}
