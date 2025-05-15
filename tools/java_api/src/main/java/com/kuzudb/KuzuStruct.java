package com.kuzudb;

import java.util.HashMap;
import java.util.Map;

public class KuzuStruct implements AutoCloseable {
    private Value structVal;

    /**
     * @return Gets the underlying Value for the struct
     */
    public Value getValue() {
        return structVal;
    }

    /**
     * Construct a struct from a value
     *
     * @param value the value to construct the struct from
     */
    public KuzuStruct(Value value) {
        structVal = value;
    }

    /**
     * Construct a struct literal from a given set of fields represented as a Java
     * map.
     *
     * @param fields: The fields of the struct, with the keys representing the field
     *                names and the values representing the field values.
     */
    public KuzuStruct(Map<String, Value> fields) {
        if (fields.isEmpty()) {
            structVal = null;
            return;
        }
        String[] fieldNames = new String[fields.size()];
        Value[] fieldValues = new Value[fields.size()];
        int idx = 0;
        for (Map.Entry<String, Value> field : fields.entrySet()) {
            fieldNames[idx] = field.getKey();
            fieldValues[idx] = field.getValue();
            ++idx;
        }
        for (Value value : fieldValues) {
            value.checkNotDestroyed();
        }
        structVal = Native.kuzu_create_struct(fieldNames, fieldValues);
    }

    /**
     * Construct a struct literal from a given set of fields. The length of the
     * fieldName/fieldValue arrays must be the same.
     *
     * @param fieldNames:  The name of the struct fields
     * @param fieldValues: The values of the struct fields
     */
    public KuzuStruct(String[] fieldNames, Value[] fieldValues) {
        if (fieldNames.length != fieldValues.length) {
            structVal = null;
            return;
        }
        if (fieldNames.length == 0) {
            structVal = null;
            return;
        }
        for (Value value : fieldValues) {
            value.checkNotDestroyed();
        }
        structVal = Native.kuzu_create_struct(fieldNames, fieldValues);
    }

    /**
     * @return The number of fields in the struct.
     * @throws RuntimeException If the struct has been destroyed.
     */
    public long getNumFields() {
        if (structVal == null) {
            return 0;
        }
        return Native.kuzu_value_get_list_size(structVal);
    }

    /**
     * Get the index of the field with the given name
     *
     * @param fieldName: The name of the field.
     * @return The index of the field with the given name, or -1 if it doesn't exist
     * @throws RuntimeException If the struct has been destroyed.
     */
    public long getIndexByFieldName(String fieldName) {
        if (structVal == null) {
            return -1;
        }
        return Native.kuzu_value_get_struct_index(structVal, fieldName);
    }

    /**
     * Get the name of the field at the given index
     *
     * @param index: The index of the field.
     * @return The name of the field at the given index
     * @throws RuntimeException If the struct has been destroyed.
     */
    public String getFieldNameByIndex(long index) {
        if (structVal == null) {
            return null;
        }
        return Native.kuzu_value_get_struct_field_name(structVal, index);
    }

    /**
     * Get the value of the field with the given name
     *
     * @param fieldName: The name of the field.
     * @return The value of the field with the given name
     *         value.
     * @throws RuntimeException If the struct has been destroyed.
     */
    public Value getValueByFieldName(String fieldName) {
        long index = getIndexByFieldName(fieldName);
        if (index < 0) {
            return null;
        }
        return getValueByIndex(index);
    }

    /**
     * Get the value of the field at the given index
     *
     * @param index: The index of the field.
     * @return The value of the field at the given index
     *         value.
     * @throws RuntimeException If the struct has been destroyed.
     */
    public Value getValueByIndex(long index) {
        if (index < 0 || index >= getNumFields()) {
            return null;
        }
        return Native.kuzu_value_get_list_element(structVal, index);
    }

    /**
     * Gets the elements the struct as a Java map. This will be truncated if the
     * number of fields doesn't fit in a 32-bit integer.
     *
     * @return the struct as a Java map
     * @throws RuntimeException
     */
    public Map<String, Value> toMap() {
        if (structVal == null) {
            return null;
        }
        Map<String, Value> ret = new HashMap<String, Value>();
        int numFields = ((Long) getNumFields()).intValue();
        for (int i = 0; i < numFields; ++i) {
            ret.put(getFieldNameByIndex(i), getValueByIndex(i));
        }
        return ret;
    }

    /**
     * Closes this object, relinquishing the underlying value
     *
     * @throws RuntimeException
     */
    public void close() {
        structVal.close();
    }
}
