package com.kuzudb;

public class KuzuList {
    private Value listVal;

    private KuzuList() {
        listVal = null;
    }

    public Value getValue() {
        return listVal;
    }

    /**
     * Construct a list from a value
     *
     * @param value the value to construct the list from
     */
    public KuzuList(Value value) {
        listVal = value;
    }

    /**
     * Construct a list from a collection.
     *
     * @param list: the collection to construct the list from
     */
    public static KuzuList createList(Value[] values) {
        KuzuList ret = new KuzuList();
        ret.listVal = Native.kuzu_create_list(values);
        return ret;
    }

    /**
     * Construct a list of a specific size populated with the default element
     *
     * @param numElements: the size of the list to construct
     */
    public static KuzuList createListWithDefaults(DataType type, long numElements) {
        KuzuList ret = new KuzuList();
        ret.listVal = Native.kuzu_create_list(type, numElements);
        return ret;
    }

    /**
     * Get the size of the list value.
     *
     * @return The size of the list value.
     * @throws ObjectRefDestroyedException If the list value has been destroyed.
     */
    public long getListSize() throws ObjectRefDestroyedException {
        listVal.checkNotDestroyed();
        return Native.kuzu_value_get_list_size(listVal);
    }

    /**
     * Get the element at the given index from the given list value.
     *
     * @param index: The index of the element.
     * @return The element at the given index from the given list value.
     * @throws ObjectRefDestroyedException If the list value has been destroyed.
     */
    public Value getListElement(long index) throws ObjectRefDestroyedException {
        listVal.checkNotDestroyed();
        return Native.kuzu_value_get_list_element(listVal, index);
    }
}
