package com.kuzudb;

/**
 * QuerySummary stores the execution time, plan, compiling time and query options of a query.
 */
public class QuerySummary {

    double cmpTime;
    double exeTime;

    /**
     * Construct a new query summary.
     * @param cmpTime: The compiling time of the query.
     * @param exeTime: The execution time of the query.
     */
    public QuerySummary(double cmpTime, double exeTime) {
        this.cmpTime = cmpTime;
        this.exeTime = exeTime;
    }

    /**
     * Get the compiling time of the query.
     * @return The compiling time of the query.
     */
    public double getCompilingTime() {
        return cmpTime;
    }

    /**
     * Get the execution time of the query.
     * @return The execution time of the query.
     */
    public double getExecutionTime() {
        return exeTime;
    }
}
