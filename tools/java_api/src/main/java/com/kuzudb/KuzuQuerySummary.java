package com.kuzudb;

public class KuzuQuerySummary {

    double cmpTime;
    double exeTime;

    public KuzuQuerySummary(double cmpTime, double exeTime) {
        this.cmpTime = cmpTime;
        this.exeTime = exeTime;
    }

    public double getCompilingTime() {
        return cmpTime;
    }

    public double getExecutionTime() {
        return exeTime;
    }
}
