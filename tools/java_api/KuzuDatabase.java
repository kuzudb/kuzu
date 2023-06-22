package tools.java_api;

public class KuzuDatabase {

    long db_ref;
    String db_path;
    long buffer_size;
    boolean destroyed = false;

    private void checkNotDestroyed () throws KuzuObjectRefDestroyedException {
        if (destroyed)
            throw new KuzuObjectRefDestroyedException("KuzuDatabase has been destroyed.");
    }

    @Override  
    protected void finalize() throws KuzuObjectRefDestroyedException {
        destroy();   
    } 
    
    public KuzuDatabase (String database_path, long buffer_pool_size) {
        this.db_path = database_path;
        this.buffer_size = buffer_pool_size;
        db_ref = KuzuNative.kuzu_database_init(database_path, buffer_pool_size);
    }

    public void destroy() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_database_destroy(this);
        destroyed = true;
    }

    public static void setLoggingLevel(String logging_level) {
        KuzuNative.kuzu_database_set_logging_level(logging_level);
    }
}
