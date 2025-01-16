package com.kuzudb;

import java.util.Map;

public class KuzuMap {
    private KuzuList mapRepresentation;

    private KuzuMap(KuzuList rep) {
        mapRepresentation = rep;
    }

    public Value getValue() {
        return mapRepresentation.getValue();
    }

    /**
     * Construct a map from a value
     *
     * @param value the value to construct the map from
     */
    public KuzuMap(Value value) {
        mapRepresentation = new KuzuList(value);
    }

    /**
     * Construct a map from a collection.
     *
     * @param map: the collection to construct the map from
     */
    public static KuzuMap createMap(Map<Value, Value> map) throws ObjectRefDestroyedException {
        // The underlying representation of a map is a list of pairs
        // The pairs are represented as a list of size 2
        Value[] mapElements = new Value[map.size()];
        int idx = 0;
        for (Map.Entry<Value, Value> entry : map.entrySet()) {
            Value[] curPair = { entry.getKey(), entry.getValue() };
            mapElements[idx] = KuzuList.createList(curPair).getValue();
            ++idx;
        }
        return new KuzuMap(KuzuList.createList(mapElements));
    }

    private Value getMapKeyOrValue(long index, boolean isKey) throws ObjectRefDestroyedException {
        if (index < 0 || index >= getNumFields()) {
            return null;
        }
        Value structValue = mapRepresentation.getListElement(index);
        Value keyOrValue = new KuzuList(structValue).getListElement(isKey ? 0 : 1);
        structValue.close();
        return keyOrValue;
    }

    /**
     * @return The number of fields in the map.
     * @throws ObjectRefDestroyedException If the map has been destroyed.
     */
    public long getNumFields() throws ObjectRefDestroyedException {
        return mapRepresentation.getListSize();
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
