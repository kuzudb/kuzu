package tools.java_api;

public class KuzuConnection {

    long conn_ref;
    boolean destoryed = false;
    public KuzuConnection(KuzuDatabase db) {
        if (db == null || db.isDestoryed()){
            // throw exception here?
        } else {
            conn_ref = KuzuNative.kuzu_connection_init(db);
        }
    }

    public void destory() {
        assert !destoryed: "Connection has been destoryed.";
        KuzuNative.kuzu_connection_destroy(this);
        destoryed = true;
    }

    public boolean isDestoryed() {
        return destoryed;
    }

    public void beginReadOnlyTransaction() {
        assert !destoryed: "Connection has been destoryed.";
        KuzuNative.kuzu_connection_begin_read_only_transaction(this);
    }

    public void beginWriteTransaction() {
        assert !destoryed: "Connection has been destoryed.";
        KuzuNative.kuzu_connection_begin_write_transaction(this);
    }

    public void commit() {
        assert !destoryed: "Connection has been destoryed.";
        KuzuNative.kuzu_connection_commit(this);
    }

    public void rollback() {
        assert !destoryed: "Connection has been destoryed.";
        KuzuNative.kuzu_connection_rollback(this);
    }

    public void setMaxNumThreadForExec(long num_threads) {
        assert !destoryed: "Connection has been destoryed.";
        KuzuNative.kuzu_connection_set_max_num_thread_for_exec(this, num_threads);
    }

    public long getMaxNumThreadForExec () {
        assert !destoryed: "Connection has been destoryed.";
        return KuzuNative.kuzu_connection_get_max_num_thread_for_exec(this);
    }

    public KuzuQueryResult query (String queryStr) {
        assert !destoryed: "Connection has been destoryed.";
        return KuzuNative.kuzu_connection_query(this, queryStr);
    }

    public KuzuPreparedStatement prepare (String queryStr) {
        // TODO: Implement prepared statements
        return null;
    }

    public KuzuQueryResult execute (KuzuPreparedStatement ps) {
        // TODO: Implement prepared statements
        return null;
    }

    public String getNodeTableNames () {
        assert !destoryed: "Connection has been destoryed.";
        return KuzuNative.kuzu_connection_get_node_table_names(this);
    }

    public String getRelTableNames () {
        assert !destoryed: "Connection has been destoryed.";
        return KuzuNative.kuzu_connection_get_rel_table_names(this);
    }

    public String getNodePropertyNames (String table_name) {
        assert !destoryed: "Connection has been destoryed.";
        return KuzuNative.kuzu_connection_get_node_property_names(this, table_name);
    }

    public String getRelPropertyNames (String table_name) {
        assert !destoryed: "Connection has been destoryed.";
        return KuzuNative.kuzu_connection_get_rel_property_names(this, table_name);
    }

    public void interrupt () {
        assert !destoryed: "Connection has been destoryed.";
        KuzuNative.kuzu_connection_interrupt(this);
    }

    public void setQueryTimeout (long timeout_in_ms) {
        assert !destoryed: "Connection has been destoryed.";
        KuzuNative.kuzu_connection_set_query_timeout(this, timeout_in_ms);
    }
}
