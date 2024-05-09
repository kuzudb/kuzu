package com.kuzudb;

/**
* The KuzuDatabase class is the main class of KuzuDB. It manages all database components.
*/
public class KuzuDatabase {
    long db_ref;
    String db_path;
    long buffer_size;
    long max_db_size;
    boolean enableCompression = true;
    boolean destroyed = false;
    boolean readOnly = false;

    /**
     * Creates a database object.
     * @param databasePath: Database path. If the database does not already exist, it will be created.
     */
    public KuzuDatabase(String databasePath) {
        this.db_path = databasePath;
        this.buffer_size = 0;
        this.max_db_size = 0;
        db_ref = KuzuNative.kuzu_database_init(databasePath, 0, true, false, max_db_size);
    }

    /**
    * Creates a database object.
    * @param databasePath: Database path. If the database does not already exist, it will be created.
    * @param bufferPoolSize: Max size of the buffer pool in bytes.
    * @param enableCompression: Enable compression in storage.
    * @param readOnly: Open the database in READ_ONLY mode.
    * @param maxDBSize: The maximum size of the database in bytes. Note that this is introduced
    * temporarily for now to get around with the default 8TB mmap address space limit some
    * environment.
    */
    public KuzuDatabase(String databasePath, long bufferPoolSize, boolean enableCompression, boolean readOnly, long maxDBSize) {
        this.db_path = databasePath;
        this.buffer_size = bufferPoolSize;
        this.enableCompression = enableCompression;
        this.readOnly = readOnly;
        this.max_db_size = maxDBSize;
        db_ref = KuzuNative.kuzu_database_init(databasePath, bufferPoolSize, enableCompression, readOnly, maxDBSize);
    }

    /**
    * Checks if the database instance has been destroyed.
    * @throws KuzuObjectRefDestroyedException If the database instance is destroyed.
    */
    private void checkNotDestroyed() throws KuzuObjectRefDestroyedException {
        if (destroyed)
            throw new KuzuObjectRefDestroyedException("KuzuDatabase has been destroyed.");
    }

    /**
    * Finalize.
    * @throws KuzuObjectRefDestroyedException If the database instance has been destroyed.
    */
    @Override
    protected void finalize() throws KuzuObjectRefDestroyedException {
        destroy();
    }

    /**
    * Destroy the database instance.
    * @throws KuzuObjectRefDestroyedException If the database instance has been destroyed.
    */
    public void destroy() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_database_destroy(this);
        destroyed = true;
    }
}
