package com.kuzudb;

/**
 * Utility functions for KuzuValue of list type.
 */
public class KuzuValueListUtil {
    /**
     * Get the size of the list value.
     * @param value: The list value.
     * @return The size of the list value.
     * @throws KuzuObjectRefDestroyedException If the list value has been destroyed.
     */
    public static long getListSize(KuzuValue value) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_value_get_list_size(value);
    }

    /**
     * Get the element at the given index from the given list value.
     * @param value: The list value.
     * @param index: The index of the element.
     * @return The element at the given index from the given list value.
     * @throws KuzuObjectRefDestroyedException If the list value has been destroyed.
     */
    public static KuzuValue getListElement(KuzuValue value, long index) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_value_get_list_element(value, index);
    }
}
