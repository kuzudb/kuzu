package com.kuzudb;

import java.util.Map;

public class KuzuMap {
    private Value mapVal;

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
     * Construct a map from a collection.
     *
     * @param map: the collection to construct the map from
     */
    public static KuzuMap createMap(Map<Value, Value> map) throws ObjectRefDestroyedException {
        if (map.isEmpty()) {
            return null;
        }

        // The underlying representation of a map is a list of pairs
        // The pairs are represented as a list of size 2
        Value[] keys = new Value[map.size()];
        Value[] values = new Value[map.size()];
        int idx = 0;
        for (Map.Entry<Value, Value> entry : map.entrySet()) {
            keys[idx] = entry.getKey();
            values[idx] = entry.getValue();
            ++idx;
        }
        return new KuzuMap(Native.kuzu_create_map(keys, values));
    }

    private Value getMapKeyOrValue(long index, boolean isKey) throws ObjectRefDestroyedException {
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
     * @throws ObjectRefDestroyedException If the map has been destroyed.
     */
    public long getNumFields() throws ObjectRefDestroyedException {
        return Native.kuzu_value_get_list_size(mapVal);
    }

    /**
     * Get the key at the given index.
     *
     * @param index: The index of the key.
     * @return The key.
     * @throws ObjectRefDestroyedException If the map has been destroyed.
     */
    public Value getKey(long index) throws ObjectRefDestroyedException {
        return getMapKeyOrValue(index, true);
    }

    /**
     * Get the value at the given index.
     *
     * @param index: The index of the value.
     * @return The value.
     * @throws ObjectRefDestroyedException If the map value has been destroyed.
     */
    public Value getValue(long index) throws ObjectRefDestroyedException {
        return getMapKeyOrValue(index, false);
    }
}
