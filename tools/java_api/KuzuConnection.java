package tools.java_api;
import java.util.Map;

public class KuzuConnection {

    long conn_ref;
    boolean destroyed = false;

    private void checkNotdestroyed () {
        assert !destroyed: "FlatTuple has been destroyed.";
    }

    public KuzuConnection(KuzuDatabase db) {
        if (db == null){
            // throw exception here?
        } else {
            conn_ref = KuzuNative.kuzu_connection_init(db);
        }
    }

    public void destory() {
        checkNotdestroyed();
        KuzuNative.kuzu_connection_destroy(this);
        destroyed = true;
    }

    public void beginReadOnlyTransaction() {
        checkNotdestroyed();
        KuzuNative.kuzu_connection_begin_read_only_transaction(this);
    }

    public void beginWriteTransaction() {
        checkNotdestroyed();
        KuzuNative.kuzu_connection_begin_write_transaction(this);
    }

    public void commit() {
        checkNotdestroyed();
        KuzuNative.kuzu_connection_commit(this);
    }

    public void rollback() {
        checkNotdestroyed();
        KuzuNative.kuzu_connection_rollback(this);
    }

    public void setMaxNumThreadForExec(long num_threads) {
        checkNotdestroyed();
        KuzuNative.kuzu_connection_set_max_num_thread_for_exec(this, num_threads);
    }

    public long getMaxNumThreadForExec () {
        checkNotdestroyed();
        return KuzuNative.kuzu_connection_get_max_num_thread_for_exec(this);
    }

    public KuzuQueryResult query (String queryStr) {
        checkNotdestroyed();
        return KuzuNative.kuzu_connection_query(this, queryStr);
    }

    public KuzuPreparedStatement prepare (String queryStr) {
        checkNotdestroyed();
        return KuzuNative.kuzu_connection_prepare(this, queryStr);
    }

    public KuzuQueryResult execute (KuzuPreparedStatement ps, Map<String, KuzuValue> m) {
        checkNotdestroyed();
        return KuzuNative.kuzu_connection_execute(this, ps, m);
    }

    public String getNodeTableNames () {
        checkNotdestroyed();
        return KuzuNative.kuzu_connection_get_node_table_names(this);
    }

    public String getRelTableNames () {
        checkNotdestroyed();
        return KuzuNative.kuzu_connection_get_rel_table_names(this);
    }

    public String getNodePropertyNames (String table_name) {
        checkNotdestroyed();
        return KuzuNative.kuzu_connection_get_node_property_names(this, table_name);
    }

    public String getRelPropertyNames (String table_name) {
        checkNotdestroyed();
        return KuzuNative.kuzu_connection_get_rel_property_names(this, table_name);
    }

    public void interrupt () {
        checkNotdestroyed();
        KuzuNative.kuzu_connection_interrupt(this);
    }

    public void setQueryTimeout (long timeout_in_ms) {
        checkNotdestroyed();
        KuzuNative.kuzu_connection_set_query_timeout(this, timeout_in_ms);
    }
}
