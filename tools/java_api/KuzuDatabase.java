package tools.java_api;

public class KuzuDatabase {

    long db_ref;
    String db_path;
    long buffer_size;
    boolean destroyed = false;

    private void checkNotdestroyed () {
        assert !destroyed: "FlatTuple has been destroyed.";
    }
    
    public KuzuDatabase (String database_path, long buffer_pool_size) {
        this.db_path = database_path;
        this.buffer_size = buffer_pool_size;
        db_ref = KuzuNative.kuzu_database_init(database_path, buffer_pool_size);
        System.out.println(db_ref);
    }

    public void destory() {
        checkNotdestroyed();
        KuzuNative.kuzu_database_destroy(this);
        destroyed = true;
    }

    public void setLoggingLevel(String logging_level, KuzuDatabase db) {
        checkNotdestroyed();
        KuzuNative.kuzu_database_set_logging_level(logging_level ,db);
    }
}
