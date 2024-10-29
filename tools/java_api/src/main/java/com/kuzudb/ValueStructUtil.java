package com.kuzudb;

/**
 * Utility functions for Value of struct type.
 */
public class ValueStructUtil {
    /**
     * Get the number of fields of the struct value.
     *
     * @param value: The struct value.
     * @return The number of fields of the struct value.
     * @throws ObjectRefDestroyedException If the struct value has been destroyed.
     */
    public static long getNumFields(Value value) throws ObjectRefDestroyedException {
        value.checkNotDestroyed();
        return Native.kuzu_value_get_list_size(value);
    }

    /**
     * Get the index of the field with the given name from the given struct value.
     *
     * @param value:     The struct value.
     * @param fieldName: The name of the field.
     * @return The index of the field with the given name from the given struct value.
     * @throws ObjectRefDestroyedException If the struct value has been destroyed.
     */
    public static long getIndexByFieldName(Value value, String fieldName) throws ObjectRefDestroyedException {
        value.checkNotDestroyed();
        return Native.kuzu_value_get_struct_index(value, fieldName);
    }

    /**
     * Get the name of the field at the given index from the given struct value.
     *
     * @param value: The struct value.
     * @param index: The index of the field.
     * @return The name of the field at the given index from the given struct value.
     * @throws ObjectRefDestroyedException If the struct value has been destroyed.
     */
    public static String getFieldNameByIndex(Value value, long index) throws ObjectRefDestroyedException {
        value.checkNotDestroyed();
        return Native.kuzu_value_get_struct_field_name(value, index);
    }

    /**
     * Get the value of the field with the given name from the given struct value.
     *
     * @param value:     The struct value.
     * @param fieldName: The name of the field.
     * @return The value of the field with the given name from the given struct value.
     * @throws ObjectRefDestroyedException If the struct value has been destroyed.
     */
    public static Value getValueByFieldName(Value value, String fieldName) throws ObjectRefDestroyedException {
        value.checkNotDestroyed();
        long index = getIndexByFieldName(value, fieldName);
        if (index < 0) {
            return null;
        }
        return getValueByIndex(value, index);
    }

    /**
     * Get the value of the field at the given index from the given struct value.
     *
     * @param value: The struct value.
     * @param index: The index of the field.
     * @return The value of the field at the given index from the given struct value.
     * @throws ObjectRefDestroyedException If the struct value has been destroyed.
     */
    public static Value getValueByIndex(Value value, long index) throws ObjectRefDestroyedException {
        value.checkNotDestroyed();
        if (index < 0 || index >= getNumFields(value)) {
            return null;
        }
        return Native.kuzu_value_get_list_element(value, index);
    }
}
