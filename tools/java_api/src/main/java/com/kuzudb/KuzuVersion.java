package com.kuzudb;

/**
 * KuzuVersion is a class to get the version of the Kùzu.
 */
public class KuzuVersion {

    /**
     * Get the version of the Kùzu.
     * @return The version of the Kùzu.
     */
    public static String getVersion() {
        return KuzuNative.kuzu_get_version();
    }

    /**
     * Get the storage version of the Kùzu.
     * @return The storage version of the Kùzu.
     */
    public static long getStorageVersion() {
        return KuzuNative.kuzu_get_storage_version();
    }
}
