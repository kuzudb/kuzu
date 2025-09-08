package com.kuzudb;

/**
 * DataType is the kuzu internal representation of data types.
 */
public class DataType implements AutoCloseable {
    long dt_ref;
    boolean destroyed = false;

    /**
     * Create a non-nested DataType object from its internal ID.
     *
     * @param id: the kuzu internal representation of data type IDs.
     */
    public DataType(DataTypeID id) {
        dt_ref = Native.kuzuDataTypeCreate(id, null, 0);
    }

    public DataType
            (DataTypeID id, DataType child_type, long num_elements_in_array) {
        dt_ref = Native.kuzuDataTypeCreate(id, child_type, num_elements_in_array);
    }

    /**
     * Checks if the database instance has been destroyed.
     *
     * @throws RuntimeException If the data type instance has been destroyed.
     */
    private void checkNotDestroyed() {
        if (destroyed)
            throw new RuntimeException("DataType has been destroyed.");
    }

    /**
     * Close the datatype and release the underlying resources. This method is invoked automatically on objects managed by the try-with-resources statement.
     *
     * @throws RuntimeException If the data type instance has been destroyed.
     */
    @Override
    public void close() {
        destroy();
    }

    /**
     * Destroy the data type instance.
     *
     * @throws RuntimeException If the data type instance has been destroyed.
     */
    private void destroy() {
        checkNotDestroyed();
        Native.kuzuDataTypeDestroy(this);
        destroyed = true;
    }

    /**
     * Clone the data type instance.
     *
     * @return The cloned data type instance.
     */
    public DataType clone() {
        if (destroyed)
            return null;
        else
            return Native.kuzuDataTypeClone(this);
    }

    /**
     * Returns true if the given data type is equal to the other data type, false otherwise.
     *
     * @param other The other data type to compare with.
     * @return If the given data type is equal to the other data type or not.
     * @throws RuntimeException If the data type instance has been destroyed.
     */
    public boolean equals(DataType other) {
        checkNotDestroyed();
        return Native.kuzuDataTypeEquals(this, other);
    }

    /**
     * Returns the enum internal id of the given data type.
     *
     * @return The enum internal id of the given data type.
     * @throws RuntimeException If the data type instance has been destroyed.
     */
    public DataTypeID getID() {
        checkNotDestroyed();
        return Native.kuzuDataTypeGetId(this);
    }

    /**
     * Returns the child type of the given data type.
     *
     * @return The child type of the given data type.
     * @throws RuntimeException If the data type instance has been destroyed.
     */
    public DataType getChildType() {
        checkNotDestroyed();
        return Native.kuzuDataTypeGetChildType(this);
    }

    /**
     * Returns the fixed number of elements in the list of the given data type.
     *
     * @return The fixed number of elements in the list of the given data type.
     * @throws RuntimeException If the data type instance has been destroyed.
     */
    public long getFixedNumElementsInList() {
        checkNotDestroyed();
        return Native.kuzuDataTypeGetNumElementsInArray(this);
    }

}
