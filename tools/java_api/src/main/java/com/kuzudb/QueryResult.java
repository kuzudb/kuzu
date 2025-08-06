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
     * @throws RuntimeException If the query result has been destroyed.
     */
    private void checkNotDestroyed() {
        if (destroyed)
            throw new RuntimeException("QueryResult has been destroyed.");
    }

    /**
     * Close the query result and release the underlying resources. This method is invoked automatically on objects managed by the try-with-resources statement.
     *
     * @throws RuntimeException If the query result has been destroyed.
     */
    @Override
    public void close() {
        destroy();
    }

    public boolean isOwnedByCPP() {
        return isOwnedByCPP;
    }

    /**
     * Destroy the query result.
     *
     * @throws RuntimeException If the query result has been destroyed.
     */
    private void destroy() {
        checkNotDestroyed();
        if (!isOwnedByCPP) {
            Native.kuzuQueryResultDestroy(this);
            destroyed = true;
        }
    }

    /**
     * Check if the query is executed successfully.
     *
     * @return Query is executed successfully or not.
     * @throws RuntimeException If the query result has been destroyed.
     */
    public boolean isSuccess() {
        checkNotDestroyed();
        return Native.kuzuQueryResultIsSuccess(this);
    }

    /**
     * Get the error message if any.
     *
     * @return Error message of the query execution if the query fails.
     * @throws RuntimeException If the query result has been destroyed.
     */
    public String getErrorMessage() {
        checkNotDestroyed();
        return Native.kuzuQueryResultGetErrorMessage(this);
    }

    /**
     * Get the number of columns in the query result.
     *
     * @return The number of columns in the query result.
     * @throws RuntimeException If the query result has been destroyed.
     */
    public long getNumColumns() {
        checkNotDestroyed();
        return Native.kuzuQueryResultGetNumColumns(this);
    }

    /**
     * Get the column name at the given index.
     *
     * @param index: The index of the column.
     * @return The column name at the given index.
     * @throws RuntimeException If the query result has been destroyed.
     */
    public String getColumnName(long index) {
        checkNotDestroyed();
        return Native.kuzuQueryResultGetColumnName(this, index);
    }

    /**
     * Get the column data type at the given index.
     *
     * @param index: The index of the column.
     * @return The column data type at the given index.
     * @throws RuntimeException If the query result has been destroyed.
     */
    public DataType getColumnDataType(long index) {
        checkNotDestroyed();
        return Native.kuzuQueryResultGetColumnDataType(this, index);
    }

    /**
     * Get the number of tuples in the query result.
     *
     * @return The number of tuples in the query result.
     * @throws RuntimeException If the query result has been destroyed.
     */
    public long getNumTuples() {
        checkNotDestroyed();
        return Native.kuzuQueryResultGetNumTuples(this);
    }

    /**
     * Get the query summary.
     *
     * @return The query summary.
     * @throws RuntimeException If the query result has been destroyed.
     */
    public QuerySummary getQuerySummary() {
        checkNotDestroyed();
        return Native.kuzuQueryResultGetQuerySummary(this);
    }

    /**
     * Return if the query result has next tuple or not.
     *
     * @return Whether there are more tuples to read.
     * @throws RuntimeException If the query result has been destroyed.
     */
    public boolean hasNext() {
        checkNotDestroyed();
        return Native.kuzuQueryResultHasNext(this);
    }

    /**
     * Get the next tuple. Note that to reduce resource allocation, all calls to
     * getNext() reuse the same FlatTuple object. Since its contents will be
     * overwritten, please complete processing a FlatTuple or make a copy of its
     * data before calling getNext() again.
     *
     * @return The next tuple.
     * @throws RuntimeException If the query result has been destroyed.
     */
    public FlatTuple getNext() {
        checkNotDestroyed();
        return Native.kuzuQueryResultGetNext(this);
    }

    /**
     * Return if the query result has next query result or not.
     *
     * @return Whether there are more query results to read.
     * @throws RuntimeException If the query result has been destroyed.
     */
    public boolean hasNextQueryResult() {
        checkNotDestroyed();
        return Native.kuzuQueryResultHasNextQueryResult(this);
    }

    /**
     * Get the next query result.
     *
     * @return The next query result.
     * @throws RuntimeException If the query result has been destroyed.
     */
    public QueryResult getNextQueryResult() {
        checkNotDestroyed();
        return Native.kuzuQueryResultGetNextQueryResult(this);
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
            return Native.kuzuQueryResultToString(this);
    }

    /**
     * Reset the query result iterator.
     *
     * @throws RuntimeException If the query result has been destroyed.
     */
    public void resetIterator() {
        checkNotDestroyed();
        Native.kuzuQueryResultResetIterator(this);
    }
}
