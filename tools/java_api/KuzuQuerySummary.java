package tools.java_api;

public class KuzuQuerySummary {
    long qs_ref;
    boolean destroyed = false;

    private void checkNotdestroyed () {
        assert !destroyed: "KuzuValue has been destroyed.";
    }

    public void destroy() {
        checkNotdestroyed();
        KuzuNative.kuzu_query_summary_destroy(this);
        destroyed = true;
    }

    public double getCompilingTime() {
        checkNotdestroyed();
        return KuzuNative.kuzu_query_summary_get_compiling_time(this);
    }

    public double getExecutionTime() {
        checkNotdestroyed();
        return KuzuNative.kuzu_query_summary_get_execution_time(this);
    }
}
