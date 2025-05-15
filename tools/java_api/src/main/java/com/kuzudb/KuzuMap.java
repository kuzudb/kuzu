package com.kuzudb;

public class KuzuMap implements AutoCloseable {
    private Value mapVal;

    /**
     * @return Gets the underlying Value for the map
     */
    public Value getValue() {
        return mapVal;
    }

    /**
     * Construct a map from a value
     *
     * @param value the value to construct the map from
     */
    public KuzuMap(Value value) {
        mapVal = value;
    }

    /**
     * Construct a map literal from a given set of keys/values. The length of the
     * key/value arrays must be the same.
     *
     * @param keys:   The keys in the map
     * @param values: The values in the map
     */
    public KuzuMap(Value[] keys, Value[] values) {
        if (keys.length != values.length) {
            mapVal = null;
            return;
        }
        if (keys.length == 0) {
            mapVal = null;
            return;
        }
        mapVal = Native.kuzu_create_map(keys, values);
    }

    private Value getMapKeyOrValue(long index, boolean isKey) {
        if (index < 0 || index >= getNumFields()) {
            return null;
        }
        Value structValue = Native.kuzu_value_get_list_element(mapVal, index);
        Value keyOrValue = new KuzuList(structValue).getListElement(isKey ? 0 : 1);
        structValue.close();
        return keyOrValue;
    }

    /**
     * @return The number of fields in the map.
     * @throws RuntimeException If the map has been destroyed.
     */
    public long getNumFields() {
        if (mapVal == null) {
            return 0;
        }
        return Native.kuzu_value_get_list_size(mapVal);
    }

    /**
     * Get the key at the given index.
     *
     * @param index: The index of the key.
     * @return The key.
     * @throws RuntimeException If the map has been destroyed.
     */
    public Value getKey(long index) {
        return getMapKeyOrValue(index, true);
    }

    /**
     * Get the value at the given index.
     *
     * @param index: The index of the value.
     * @return The value.
     * @throws RuntimeException If the map value has been destroyed.
     */
    public Value getValue(long index) {
        return getMapKeyOrValue(index, false);
    }

    /**
     * Closes this object, relinquishing the underlying value
     *
     * @throws RuntimeException
     */
    public void close() {
        mapVal.close();
    }
}
