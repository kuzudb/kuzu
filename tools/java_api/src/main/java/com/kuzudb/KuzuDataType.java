package com.kuzudb;

/**
* KuzuDataType is the kuzu internal representation of data types.
*/
public class KuzuDataType {
    long dt_ref;
    boolean destroyed = false;

    /**
    * Create a non-nested KuzuDataType object from its internal ID.
    * @param id: the kuzu internal representation of data type IDs.
    */
    public KuzuDataType(KuzuDataTypeID id) {
        dt_ref = KuzuNative.kuzu_data_type_create(id, null, 0);
    }

    public KuzuDataType
            (KuzuDataTypeID id, KuzuDataType child_type, long num_elements_in_array) {
        dt_ref = KuzuNative.kuzu_data_type_create(id, child_type, num_elements_in_array);
    }

    /**
     * Checks if the database instance has been destroyed.
     * @throws KuzuObjectRefDestroyedException If the data type instance has been destroyed.
    */
    private void checkNotDestroyed() throws KuzuObjectRefDestroyedException {
        if (destroyed)
            throw new KuzuObjectRefDestroyedException("KuzuDataType has been destroyed.");
    }

    /**
    * Finalize.
    * @throws KuzuObjectRefDestroyedException If the data type instance has been destroyed.
    */
    @Override
    protected void finalize() throws KuzuObjectRefDestroyedException {
        destroy();
    }

    /**
    * Destroy the data type instance.
    * @throws KuzuObjectRefDestroyedException If the data type instance has been destroyed.
    */
    public void destroy() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_data_type_destroy(this);
        destroyed = true;
    }

    /**
    * Clone the data type instance.
    * @return The cloned data type instance.
    */
    public KuzuDataType clone() {
        if (destroyed)
            return null;
        else
            return KuzuNative.kuzu_data_type_clone(this);
    }

    /**
    * Returns true if the given data type is equal to the other data type, false otherwise.
    * @param other The other data type to compare with.
    * @return If the given data type is equal to the other data type or not.
    * @throws KuzuObjectRefDestroyedException If the data type instance has been destroyed.
    */
    public boolean equals(KuzuDataType other) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_data_type_equals(this, other);
    }

    /**
    * Returns the enum internal id of the given data type.
    * @return The enum internal id of the given data type.
    * @throws KuzuObjectRefDestroyedException If the data type instance has been destroyed.
    */
    public KuzuDataTypeID getID() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_data_type_get_id(this);
    }

    /**
    * Returns the child type of the given data type.
    * @return The child type of the given data type.
    * @throws KuzuObjectRefDestroyedException If the data type instance has been destroyed.
    */
    public KuzuDataType getChildType() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_data_type_get_child_type(this);
    }

    /**
    * Returns the fixed number of elements in the list of the given data type.
    * @return The fixed number of elements in the list of the given data type.
    * @throws KuzuObjectRefDestroyedException If the data type instance has been destroyed.
    */
    public long getFixedNumElementsInList() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_data_type_get_num_elements_in_array(this);
    }

}
