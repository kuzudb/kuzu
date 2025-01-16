package com.kuzudb;

public class KuzuStruct {
    private KuzuList structRepresentation;

    public Value getValue() {
        return structRepresentation.getValue();
    }

    /**
     * Construct a map from a value
     *
     * @param value the value to construct the map from
     */
    public KuzuStruct(Value value) {
        structRepresentation = new KuzuList(value);
    }

    /**
     * @return The number of fields in the map.
     * @throws ObjectRefDestroyedException If the map has been destroyed.
     */
    public long getNumFields() throws ObjectRefDestroyedException {
        return structRepresentation.getListSize();
    }

    /**
     * Get the index of the field with the given name
     *
     * @param fieldName: The name of the field.
     * @return The index of the field with the given name
     * @throws ObjectRefDestroyedException If the struct has been destroyed.
     */
    public long getIndexByFieldName(String fieldName) throws ObjectRefDestroyedException {
        return Native.kuzu_value_get_struct_index(structRepresentation.getValue(), fieldName);
    }

    /**
     * Get the name of the field at the given index
     *
     * @param index: The index of the field.
     * @return The name of the field at the given index
     * @throws ObjectRefDestroyedException If the struct has been destroyed.
     */
    public String getFieldNameByIndex(long index) throws ObjectRefDestroyedException {
        return Native.kuzu_value_get_struct_field_name(structRepresentation.getValue(), index);
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
        return structRepresentation.getListElement(index);
    }
}
