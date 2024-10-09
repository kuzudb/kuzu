package com.kuzudb;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        return Native.kuzu_value_get_map_index(value, fieldName);
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
        if (index < 0) {
            return null;
        }
        return getValueByIndex(value, index);
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

    public static String extractKey(Value map_value) {
        String map_string = map_value.toString();
        Pattern keyPattern = Pattern.compile("KEY:\\s*(\\S+),\\s+VALUE:");
        Matcher keyMatcher = keyPattern.matcher(map_string);

        if (keyMatcher.find()) {
            return keyMatcher.group(1);  // Return the captured key
        }
        return null;
    }

    public static int extractValue(Value map_value) {
        String map_string = map_value.toString();
        Pattern valuePattern = Pattern.compile("VALUE:\\s*(\\S+)");
        Matcher valueMatcher = valuePattern.matcher(map_string);

        int int_value = Integer.parseInt(valueMatcher.group(1));
        return int_value;
    }
}
