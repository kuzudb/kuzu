package com.kuzudb;

public class KuzuList implements AutoCloseable {
    private Value listVal;

    /**
     * @return Gets the underlying Value for the list
     */
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
     * Construct a list literal from an array of values
     *
     * @param values: the array to construct the list from
     */
    public KuzuList(Value[] values) {
        listVal = Native.kuzu_create_list(values);
    }

    /**
     * Construct a list of a specific size populated with the default element
     *
     * @param numElements: the size of the list to construct
     */
    public KuzuList(DataType type, long numElements) {
        listVal = Native.kuzu_create_list(type, numElements);
    }

    /**
     * Get the size of the list.
     *
     * @return The size of the list.
     * @throws ObjectRefDestroyedException If the list has been destroyed.
     */
    public long getListSize() throws ObjectRefDestroyedException {
        listVal.checkNotDestroyed();
        return Native.kuzu_value_get_list_size(listVal);
    }

    /**
     * Get the element at the given index from the given list.
     *
     * @param index: The index of the element.
     * @return The element at the given index from the given list.
     * @throws ObjectRefDestroyedException If the list has been destroyed.
     */
    public Value getListElement(long index) throws ObjectRefDestroyedException {
        listVal.checkNotDestroyed();
        return Native.kuzu_value_get_list_element(listVal, index);
    }

    /**
     * Gets the elements the list as a Java array. This will be truncated if the
     * size of the list doesn't fit in a 32-bit integer.
     *
     * @return the list as a Java array
     * @throws ObjectRefDestroyedException
     */
    public Value[] toArray() throws ObjectRefDestroyedException {
        int arraySize = ((Long) getListSize()).intValue();
        Value[] ret = new Value[arraySize];
        for (int i = 0; i < arraySize; ++i) {
            ret[i] = getListElement(i);
        }
        return ret;
    }

    /**
     * Closes this object, relinquishing the underlying value
     *
     * @throws ObjectRefDestroyedException
     */
    public void close() throws ObjectRefDestroyedException {
        listVal.close();
    }
}
