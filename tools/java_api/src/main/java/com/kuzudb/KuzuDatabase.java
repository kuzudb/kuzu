package com.kuzudb;

/**
* The KuzuDatabase class is the main class of KuzuDB. It manages all database components.
*/
public class KuzuDatabase {
    public enum AccessMode {
        READ_ONLY(0),
        READ_WRITE(1);

        private final int value;

        AccessMode(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    long db_ref;
    String db_path;
    long buffer_size;
    boolean enableCompression = true;
    boolean destroyed = false;
    AccessMode accessMode = AccessMode.READ_WRITE;

    /**
     * Creates a database object.
     * @param databasePath: Database path. If the database does not already exist, it will be created.
     */
    public KuzuDatabase(String databasePath) {
        this.db_path = databasePath;
        this.buffer_size = 0;
        db_ref = KuzuNative.kuzu_database_init(databasePath, 0, true, AccessMode.READ_WRITE.getValue());
    }

    /**
    * Creates a database object.
    * @param databasePath: Database path. If the database does not already exist, it will be created.
    * @param bufferPoolSize: Max size of the buffer pool in bytes.
    * @param enableCompression: Enable compression in storage.
    */
    public KuzuDatabase(String databasePath, long bufferPoolSize, boolean enableCompression, AccessMode accessMode) {
        this.db_path = databasePath;
        this.buffer_size = bufferPoolSize;
        this.enableCompression = enableCompression;
        this.accessMode = accessMode;
        int accessModeValue = accessMode.getValue();
        db_ref = KuzuNative.kuzu_database_init(databasePath, bufferPoolSize, enableCompression, accessModeValue);
    }

    /**
    * Sets the logging level of the database instance.
    * @param loggingLevel: New logging level. (Supported logging levels are: 'info', 'debug', 'err').
    */
    public static void setLoggingLevel(String loggingLevel) {
        KuzuNative.kuzu_database_set_logging_level(loggingLevel);
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
