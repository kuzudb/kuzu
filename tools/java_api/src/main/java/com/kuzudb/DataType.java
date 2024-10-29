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
        dt_ref = Native.kuzu_data_type_create(id, null, 0);
    }

    public DataType
            (DataTypeID id, DataType child_type, long num_elements_in_array) {
        dt_ref = Native.kuzu_data_type_create(id, child_type, num_elements_in_array);
    }

    /**
     * Checks if the database instance has been destroyed.
     *
     * @throws ObjectRefDestroyedException If the data type instance has been destroyed.
     */
    private void checkNotDestroyed() throws ObjectRefDestroyedException {
        if (destroyed)
            throw new ObjectRefDestroyedException("DataType has been destroyed.");
    }

    /**
     * Close the datatype and release the underlying resources. This method is invoked automatically on objects managed by the try-with-resources statement.
     *
     * @throws ObjectRefDestroyedException If the data type instance has been destroyed.
     */
    @Override
    public void close() throws ObjectRefDestroyedException {
        destroy();
    }

    /**
     * Destroy the data type instance.
     *
     * @throws ObjectRefDestroyedException If the data type instance has been destroyed.
     */
    private void destroy() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        Native.kuzu_data_type_destroy(this);
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
            return Native.kuzu_data_type_clone(this);
    }

    /**
     * Returns true if the given data type is equal to the other data type, false otherwise.
     *
     * @param other The other data type to compare with.
     * @return If the given data type is equal to the other data type or not.
     * @throws ObjectRefDestroyedException If the data type instance has been destroyed.
     */
    public boolean equals(DataType other) throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_data_type_equals(this, other);
    }

    /**
     * Returns the enum internal id of the given data type.
     *
     * @return The enum internal id of the given data type.
     * @throws ObjectRefDestroyedException If the data type instance has been destroyed.
     */
    public DataTypeID getID() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_data_type_get_id(this);
    }

    /**
     * Returns the child type of the given data type.
     *
     * @return The child type of the given data type.
     * @throws ObjectRefDestroyedException If the data type instance has been destroyed.
     */
    public DataType getChildType() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_data_type_get_child_type(this);
    }

    /**
     * Returns the fixed number of elements in the list of the given data type.
     *
     * @return The fixed number of elements in the list of the given data type.
     * @throws ObjectRefDestroyedException If the data type instance has been destroyed.
     */
    public long getFixedNumElementsInList() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_data_type_get_num_elements_in_array(this);
    }

}
