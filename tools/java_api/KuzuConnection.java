package tools.java_api;
import java.util.Map;

public class KuzuConnection {

    long conn_ref;
    boolean destroyed = false;

    private void checkNotDestroyed () throws KuzuObjectRefDestroyedException {
        if (destroyed)
            throw new KuzuObjectRefDestroyedException("KuzuConnection has been destroyed.");
    }

    @Override  
    protected void finalize() throws KuzuObjectRefDestroyedException {
        destroy();   
    } 

    public KuzuConnection(KuzuDatabase db) {
        assert db != null: "Cannot create connection, database is null.";
        conn_ref = KuzuNative.kuzu_connection_init(db);
    }  

    public void destroy() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_connection_destroy(this);
        destroyed = true;
    }

    public void beginReadOnlyTransaction() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_connection_begin_read_only_transaction(this);
    }

    public void beginWriteTransaction() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_connection_begin_write_transaction(this);
    }

    public void commit() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_connection_commit(this);
    }

    public void rollback() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_connection_rollback(this);
    }

    public void setMaxNumThreadForExec(long num_threads) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_connection_set_max_num_thread_for_exec(this, num_threads);
    }

    public long getMaxNumThreadForExec () throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_connection_get_max_num_thread_for_exec(this);
    }

    public KuzuQueryResult query (String queryStr) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_connection_query(this, queryStr);
    }

    public KuzuPreparedStatement prepare (String queryStr) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_connection_prepare(this, queryStr);
    }

    public KuzuQueryResult execute (KuzuPreparedStatement ps, Map<String, KuzuValue> m) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_connection_execute(this, ps, m);
    }

    public String getNodeTableNames () throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_connection_get_node_table_names(this);
    }

    public String getRelTableNames () throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_connection_get_rel_table_names(this);
    }

    public String getNodePropertyNames (String table_name) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_connection_get_node_property_names(this, table_name);
    }

    public String getRelPropertyNames (String table_name) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_connection_get_rel_property_names(this, table_name);
    }

    public void interrupt () throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_connection_interrupt(this);
    }

    public void setQueryTimeout (long timeout_in_ms) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_connection_set_query_timeout(this, timeout_in_ms);
    }
}
