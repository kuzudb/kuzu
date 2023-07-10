package com.kuzudb;

/**
 * Utility functions for KuzuValue of struct type.
 */
public class KuzuValueStructUtil {
    /**
     * Get the number of fields of the struct value.
     * @param value: The struct value.
     * @return The number of fields of the struct value.
     * @throws KuzuObjectRefDestroyedException If the struct value has been destroyed.
     */
    public static long getNumFields(KuzuValue value) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_value_get_list_size(value);
    }

    /**
     * Get the index of the field with the given name from the given struct value.
     * @param value: The struct value.
     * @param fieldName: The name of the field.
     * @return The index of the field with the given name from the given struct value.
     * @throws KuzuObjectRefDestroyedException If the struct value has been destroyed.
     */
    public static long getIndexByFieldName(KuzuValue value, String fieldName) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_value_get_struct_index(value, fieldName);
    }

    /**
     * Get the name of the field at the given index from the given struct value.
     * @param value: The struct value.
     * @param index: The index of the field.
     * @return The name of the field at the given index from the given struct value.
     * @throws KuzuObjectRefDestroyedException If the struct value has been destroyed.
     */
    public static String getFieldNameByIndex(KuzuValue value, long index) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_value_get_struct_field_name(value, index);
    }

    /**
     * Get the value of the field with the given name from the given struct value.
     * @param value: The struct value.
     * @param fieldName: The name of the field.
     * @return The value of the field with the given name from the given struct value.
     * @throws KuzuObjectRefDestroyedException If the struct value has been destroyed.
     */
    public static KuzuValue getValueByFieldName(KuzuValue value, String fieldName) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        long index = getIndexByFieldName(value, fieldName);
        if (index < 0) {
            return null;
        }
        return getValueByIndex(value, index);
    }

    /**
     * Get the value of the field at the given index from the given struct value.
     * @param value: The struct value.
     * @param index: The index of the field.
     * @return The value of the field at the given index from the given struct value.
     * @throws KuzuObjectRefDestroyedException If the struct value has been destroyed.
     */
    public static KuzuValue getValueByIndex(KuzuValue value, long index) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        if (index < 0 || index >= getNumFields(value)) {
            return null;
        }
        return KuzuNative.kuzu_value_get_list_element(value, index);
    }
}
