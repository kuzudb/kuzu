package com.kuzudb;

public class KuzuStruct {
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
     * Construct a struct literal from a given set of fields. The length of the
     * fieldName/fieldValue arrays must be the same.
     *
     * @param fieldNames:  The name of the struct fields
     * @param fieldValues: The values of the struct fields
     */
    public static KuzuStruct createStruct(String[] fieldNames, Value[] fieldValues) throws ObjectRefDestroyedException {
        if (fieldNames.length != fieldValues.length) {
            return null;
        }
        if (fieldNames.length == 0) {
            return null;
        }
        for (Value value : fieldValues) {
            value.checkNotDestroyed();
        }
        return new KuzuStruct(Native.kuzu_create_struct(fieldNames, fieldValues));
    }

    /**
     * @return The number of fields in the struct.
     * @throws ObjectRefDestroyedException If the struct has been destroyed.
     */
    public long getNumFields() throws ObjectRefDestroyedException {
        return Native.kuzu_value_get_list_size(structVal);
    }

    /**
     * Get the index of the field with the given name
     *
     * @param fieldName: The name of the field.
     * @return The index of the field with the given name
     * @throws ObjectRefDestroyedException If the struct has been destroyed.
     */
    public long getIndexByFieldName(String fieldName) throws ObjectRefDestroyedException {
        return Native.kuzu_value_get_struct_index(structVal, fieldName);
    }

    /**
     * Get the name of the field at the given index
     *
     * @param index: The index of the field.
     * @return The name of the field at the given index
     * @throws ObjectRefDestroyedException If the struct has been destroyed.
     */
    public String getFieldNameByIndex(long index) throws ObjectRefDestroyedException {
        return Native.kuzu_value_get_struct_field_name(structVal, index);
    }

    /**
     * Get the value of the field with the given name
     *
     * @param value:     The struct value.
     * @param fieldName: The name of the field.
     * @return The value of the field with the given name
     *         value.
     * @throws ObjectRefDestroyedException If the struct has been destroyed.
     */
    public Value getValueByFieldName(String fieldName) throws ObjectRefDestroyedException {
        long index = getIndexByFieldName(fieldName);
        if (index < 0) {
            return null;
        }
        return getValueByIndex(index);
    }

    /**
     * Get the value of the field at the given index
     *
     * @param value: The struct value.
     * @param index: The index of the field.
     * @return The value of the field at the given index
     *         value.
     * @throws ObjectRefDestroyedException If the struct has been destroyed.
     */
    public Value getValueByIndex(long index) throws ObjectRefDestroyedException {
        if (index < 0 || index >= getNumFields()) {
            return null;
        }
        return Native.kuzu_value_get_list_element(structVal, index);
    }
}
