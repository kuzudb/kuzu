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

    public void destoryConnection() {
        assert !destoryed: "Connection has been destoryed.";
        KuzuNative.kuzu_connection_destroy(this);
        destoryed = true;
    }

    public boolean isDestoryed() {
        return destoryed;
    }

    public void beginReadOnlyTransaction() {
        KuzuNative.kuzu_connection_begin_read_only_transaction(this);
    }

    public void beginWriteTransaction() {
        KuzuNative.kuzu_connection_begin_write_transaction(this);
    }

    public void commit() {
        KuzuNative.kuzu_connection_commit(this);
    }

    public void rollback() {
        KuzuNative.kuzu_connection_rollback(this);
    }

    public void setMaxNumThreadForExec(long num_threads) {
        KuzuNative.kuzu_connection_set_max_num_thread_for_exec(this, num_threads);
    }

    public long getMaxNumThreadForExec () {
        return KuzuNative.kuzu_connection_get_max_num_thread_for_exec(this);
    }

    public KuzuQueryResult query (String queryStr) {
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
        return KuzuNative.kuzu_connection_get_node_table_names(this);
    }

    public String getRelTableNames () {
        return KuzuNative.kuzu_connection_get_rel_table_names(this);
    }

    public String getNodePropertyNames (String table_name) {
        return KuzuNative.kuzu_connection_get_node_property_names(this, table_name);
    }

    public String getRelPropertyNames (String table_name) {
        return KuzuNative.kuzu_connection_get_rel_property_names(this, table_name);
    }

    public void interrupt () {
        KuzuNative.kuzu_connection_interrupt(this);
    }

    public void setQueryTimeout (long timeout_in_ms) {
        KuzuNative.kuzu_connection_set_query_timeout(this, timeout_in_ms);
    }
}
