package com.kuzudb;

/**
 * Version is a class to get the version of the Kuzu.
 */
public class Version {

    /**
     * Get the version of the Kuzu.
     *
     * @return The version of the Kuzu.
     */
    public static String getVersion() {
        return Native.kuzu_get_version();
    }

    /**
     * Get the storage version of the Kuzu.
     *
     * @return The storage version of the Kuzu.
     */
    public static long getStorageVersion() {
        return Native.kuzu_get_storage_version();
    }
}
