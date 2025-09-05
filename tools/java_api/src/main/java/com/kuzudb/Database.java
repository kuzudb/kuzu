package com.kuzudb;

/**
 * The Database class is the main class of KuzuDB. It manages all database
 * components.
 */
public class Database implements AutoCloseable {
    long db_ref;
    String db_path;
    long buffer_size;
    long max_db_size;
    boolean enableCompression = true;
    boolean destroyed = false;
    boolean readOnly = false;
    boolean autoCheckpoint = true;
    boolean throwOnWalReplayFailure = true;
    boolean enableChecksums = true;
    long checkpointThreshold;

    /**
     * Creates a database object. The database will be created in memory with
     * default settings.
     */
    public Database() {
        this("");
    }

    /**
     * Creates a database object.
     *
     * @param databasePath: Database path. If the database does not already exist,
     *                      it will be created.
     */
    public Database(String databasePath) {
        this.db_path = databasePath;
        this.buffer_size = 0;
        this.max_db_size = 0;
        this.checkpointThreshold = -1;
        db_ref = Native.kuzuDatabaseInit(databasePath, 0, true, false, max_db_size, autoCheckpoint,
                checkpointThreshold, throwOnWalReplayFailure, enableChecksums);
    }

    /**
     * Creates a database object.
     *
     * @param databasePath:             Database path. If the path is empty, or equal to
     *                                  `:memory:`, the database will be created in
     *                                  memory.
     * @param bufferPoolSize:           Max size of the buffer pool in bytes.
     * @param enableCompression:        Enable compression in storage.
     * @param readOnly:                 Open the database in READ_ONLY mode.
     * @param maxDBSize:                The maximum size of the database in bytes. Note
     *                                  that this is introduced
     *                                  temporarily for now to get around with the default
     *                                  8TB mmap address space limit some
     *                                  environment.
     * @param autoCheckpoint            If true, the database will automatically
     *                                  checkpoint when the size of
     *                                  the WAL file exceeds the checkpoint threshold.
     * @param checkpointThreshold       The threshold of the WAL file size in bytes. When
     *                                  the size of the
     *                                  WAL file exceeds this threshold, the database will
     *                                  checkpoint if autoCheckpoint is true.
     * @param throwOnWalReplayFailure   If true, any WAL replaying failure when loading the database
     *                                  will throw an error. Otherwise, Kuzu will silently ignore
     *                                  the failure and replay up to where the error occured.
     * @param enableChecksums           If true, the database will use checksums to detect
     *                                  corruption in the WAL file.
     */
    public Database(String databasePath, long bufferPoolSize, boolean enableCompression, boolean readOnly,
            long maxDBSize, boolean autoCheckpoint, long checkpointThreshold, boolean throwOnWalReplayFailure, boolean enableChecksums) {
        this.db_path = databasePath;
        this.buffer_size = bufferPoolSize;
        this.enableCompression = enableCompression;
        this.readOnly = readOnly;
        this.max_db_size = maxDBSize;
        this.autoCheckpoint = autoCheckpoint;
        this.checkpointThreshold = checkpointThreshold;
        this.throwOnWalReplayFailure = throwOnWalReplayFailure;
        this.enableChecksums = enableChecksums;
        db_ref = Native.kuzuDatabaseInit(databasePath, bufferPoolSize, enableCompression, readOnly, maxDBSize,
                autoCheckpoint, checkpointThreshold, throwOnWalReplayFailure, enableChecksums);
    }

    /**
     * Checks if the database instance has been destroyed.
     *
     * @throws RuntimeException If the database instance is destroyed.
     */
    private void checkNotDestroyed() {
        if (destroyed)
            throw new RuntimeException("Database has been destroyed.");
    }

    /**
     * Close the database and release the underlying resources. This method is
     * invoked automatically on objects managed by the try-with-resources statement.
     *
     * @throws RuntimeException If the database instance has been
     *                                     destroyed.
     */
    @Override
    public void close() {
        destroy();
    }

    /**
     * Destroy the database instance.
     *
     * @throws RuntimeException If the database instance has been
     *                                     destroyed.
     */
    private void destroy() {
        checkNotDestroyed();
        Native.kuzuDatabaseDestroy(this);
        destroyed = true;
    }
}
