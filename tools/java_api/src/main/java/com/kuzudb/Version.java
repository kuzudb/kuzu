package com.kuzudb;

/**
 * Version is a class to get the version of the Kùzu.
 */
public class Version {

    /**
     * Get the version of the Kùzu.
     * @return The version of the Kùzu.
     */
    public static String getVersion() {
        return Native.kuzu_get_version();
    }

    /**
     * Get the storage version of the Kùzu.
     * @return The storage version of the Kùzu.
     */
    public static long getStorageVersion() {
        return Native.kuzu_get_storage_version();
    }
}
