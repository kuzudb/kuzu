package com.kuzudb;

/**
 * Utility functions for Value of list type.
 */
public class ValueListUtil {
    /**
     * Get the size of the list value.
     * @param value: The list value.
     * @return The size of the list value.
     * @throws ObjectRefDestroyedException If the list value has been destroyed.
     */
    public static long getListSize(Value value) throws ObjectRefDestroyedException {
        value.checkNotDestroyed();
        return Native.kuzu_value_get_list_size(value);
    }

    /**
     * Get the element at the given index from the given list value.
     * @param value: The list value.
     * @param index: The index of the element.
     * @return The element at the given index from the given list value.
     * @throws ObjectRefDestroyedException If the list value has been destroyed.
     */
    public static Value getListElement(Value value, long index) throws ObjectRefDestroyedException {
        value.checkNotDestroyed();
        return Native.kuzu_value_get_list_element(value, index);
    }
}
