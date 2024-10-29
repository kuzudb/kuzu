package com.kuzudb;

/**
 * The Database class is the main class of KuzuDB. It manages all database components.
 */
public class Database implements AutoCloseable {
    long db_ref;
    String db_path;
    long buffer_size;
    long max_db_size;
    boolean enableCompression = true;
    boolean destroyed = false;
    boolean readOnly = false;

    /**
     * Creates a database object. The database will be created in memory with default settings.
     */
    public Database() {
        this("");
    }

    /**
     * Creates a database object.
     *
     * @param databasePath: Database path. If the database does not already exist, it will be created.
     */
    public Database(String databasePath) {
        this.db_path = databasePath;
        this.buffer_size = 0;
        this.max_db_size = 0;
        db_ref = Native.kuzu_database_init(databasePath, 0, true, false, max_db_size);
    }

    /**
     * Creates a database object.
     *
     * @param databasePath:      Database path. If the path is empty, or equal to `:memory:`, the database will be created in
     *                           memory.
     * @param bufferPoolSize:    Max size of the buffer pool in bytes.
     * @param enableCompression: Enable compression in storage.
     * @param readOnly:          Open the database in READ_ONLY mode.
     * @param maxDBSize:         The maximum size of the database in bytes. Note that this is introduced
     *                           temporarily for now to get around with the default 8TB mmap address space limit some
     *                           environment.
     */
    public Database(String databasePath, long bufferPoolSize, boolean enableCompression, boolean readOnly, long maxDBSize) {
        this.db_path = databasePath;
        this.buffer_size = bufferPoolSize;
        this.enableCompression = enableCompression;
        this.readOnly = readOnly;
        this.max_db_size = maxDBSize;
        db_ref = Native.kuzu_database_init(databasePath, bufferPoolSize, enableCompression, readOnly, maxDBSize);
    }

    /**
     * Checks if the database instance has been destroyed.
     *
     * @throws ObjectRefDestroyedException If the database instance is destroyed.
     */
    private void checkNotDestroyed() throws ObjectRefDestroyedException {
        if (destroyed)
            throw new ObjectRefDestroyedException("Database has been destroyed.");
    }

    /**
     * Close the database and release the underlying resources. This method is invoked automatically on objects managed by the try-with-resources statement.
     *
     * @throws ObjectRefDestroyedException If the database instance has been destroyed.
     */
    @Override
    public void close() throws ObjectRefDestroyedException {
        destroy();
    }

    /**
     * Destroy the database instance.
     *
     * @throws ObjectRefDestroyedException If the database instance has been destroyed.
     */
    private void destroy() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        Native.kuzu_database_destroy(this);
        destroyed = true;
    }
}
