package com.kuzudb;

/**
 * Utility functions for Value of map type.
 */
public class ValueMapUtil {
    /**
     * Get the number of fields of the map value.
     * @param value: The map value.
     * @return The number of fields of the map value.
     * @throws ObjectRefDestroyedException If the map value has been destroyed.
     */
    public static long getNumFields(Value value) throws ObjectRefDestroyedException {
        value.checkNotDestroyed();
        return Native.kuzu_value_get_list_size(value);
    }

    /**
     * Get the index of the field with the given name from the given map value.
     * @param value: The map value.
     * @param fieldName: The name of the field.
     * @return The index of the field with the given name from the given map value.
     * @throws ObjectRefDestroyedException If the map value has been destroyed.
     */
    public static long getIndexByFieldName(Value value, String fieldName) throws ObjectRefDestroyedException {
        value.checkNotDestroyed();
        for (long i = 0; i < getNumFields(value); i++) {
            if (fieldName.equals(getFieldNameByIndex(value, i))) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Get the name of the field at the given index from the given map value.
     * @param value: The map value.
     * @param index: The index of the field.
     * @return The name of the field at the given index from the given map value.
     * @throws ObjectRefDestroyedException If the map value has been destroyed.
     */
    public static String getFieldNameByIndex(Value value, long index) throws ObjectRefDestroyedException {
        value.checkNotDestroyed();
        return Native.kuzu_value_get_map_field_name(value, index);
    }

    /**
     * Get the value of the field with the given name from the given map value.
     * @param value: The map value.
     * @param fieldName: The name of the field.
     * @return The value of the field with the given name from the given map value.
     * @throws ObjectRefDestroyedException If the map value has been destroyed.
     */
    public static Value getValueByFieldName(Value value, String fieldName) throws ObjectRefDestroyedException {
        value.checkNotDestroyed();
        long index = getIndexByFieldName(value, fieldName);
        if (index == -1) {
            return null;
        }
        return Native.kuzu_value_get_map_value(value, index);
    }

    /**
     * Get the value of the field at the given index from the given map value.
     * @param value: The map value.
     * @param index: The index of the field.
     * @return The value of the field at the given index from the given map value.
     * @throws ObjectRefDestroyedException If the map value has been destroyed.
     */
    public static Value getValueByIndex(Value value, long index) throws ObjectRefDestroyedException {
        value.checkNotDestroyed();
        if (index < 0 || index >= getNumFields(value)) {
            return null;
        }
        return Native.kuzu_value_get_map_value(value, index);
    }
}
