package com.kuzudb;

/**
 * Utility functions for Value of map type.
 */
public class ValueMapUtil {

    private static Value getMapKeyOrValue(Value value, long index, boolean isKey) throws ObjectRefDestroyedException {
        value.checkNotDestroyed();
        if (index < 0 || index >= getNumFields(value)) {
            return null;
        }
        Value structValue = Native.kuzu_value_get_list_element(value, index);
        Value keyOrValue = Native.kuzu_value_get_list_element(structValue, isKey ? 0 : 1);
        structValue.close();
        return keyOrValue;
    }

    /**
     * Get the number of fields of the map value.
     *
     * @param value: The map value.
     * @return The number of fields of the map value.
     * @throws ObjectRefDestroyedException If the map value has been destroyed.
     */
    public static long getNumFields(Value value) throws ObjectRefDestroyedException {
        value.checkNotDestroyed();
        return Native.kuzu_value_get_list_size(value);
    }


    /**
     * Get the key from the given map value by the given index.
     *
     * @param value: The map value.
     * @param index: The index of the key.
     * @return The key from the given map value by the given index.
     * @throws ObjectRefDestroyedException If the map value has been destroyed.
     */
    public static Value getKey(Value value, long index) throws ObjectRefDestroyedException {
        return getMapKeyOrValue(value, index, true);
    }

    /**
     * Get the value from the given map value by the given index.
     *
     * @param value: The map value.
     * @param index: The index of the value.
     * @return The value from the given map value by the given index.
     * @throws ObjectRefDestroyedException If the map value has been destroyed.
     */
    public static Value getValue(Value value, long index) throws ObjectRefDestroyedException {
        return getMapKeyOrValue(value, index, false);
    }
}
