package com.kuzudb;

/**
 * Value can hold data of different types.
 */
public class Value implements AutoCloseable {
    public long v_ref;
    boolean destroyed = false;
    boolean isOwnedByCPP = false;

    /**
     * Construct a Value from a val.
     *
     * @throws ObjectRefDestroyedException If the Value has been destroyed.
     */
    public <T> Value(T val) throws ObjectRefDestroyedException {
        checkNotDestroyed();
        v_ref = Native.kuzu_value_create_value(val);
    }

    /**
     * Create a null Value.
     *
     * @return The null Value.
     */
    public static Value createNull() {
        return Native.kuzu_value_create_null();
    }

    /**
     * Create a null Value with the given data type.
     *
     * @param data_type: The data type of the null Value.
     */
    public static Value createNullWithDataType(DataType data_type) {
        return Native.kuzu_value_create_null_with_data_type(data_type);
    }

    /**
     * Create a default Value with the given data type.
     *
     * @param data_type: The data type of the default Value.
     * @return The default Value.
     */
    public static Value createDefault(DataType data_type) {
        return Native.kuzu_value_create_default(data_type);
    }

    /**
     * Check if the Value has been destroyed.
     *
     * @throws ObjectRefDestroyedException If the Value has been destroyed.
     */
    public void checkNotDestroyed() throws ObjectRefDestroyedException {
        if (destroyed)
            throw new ObjectRefDestroyedException("Value has been destroyed.");
    }

    /**
     * Close the value and release the underlying resources. This method is invoked automatically on objects managed by the try-with-resources statement.
     *
     * @throws ObjectRefDestroyedException If the Value has been destroyed.
     */
    @Override
    public void close() throws ObjectRefDestroyedException {
        destroy();
    }

    public boolean isOwnedByCPP() {
        return isOwnedByCPP;
    }

    /**
     * Destroy the Value.
     *
     * @throws ObjectRefDestroyedException If the Value has been destroyed.
     */
    private void destroy() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        if (!isOwnedByCPP) {
            Native.kuzu_value_destroy(this);
            destroyed = true;
        }
    }

    /**
     * Check if the Value is null.
     *
     * @return True if the Value is null, false otherwise.
     * @throws ObjectRefDestroyedException If the Value has been destroyed.
     */
    public boolean isNull() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_value_is_null(this);
    }

    /**
     * Set the Value to null.
     *
     * @param flag: True if the Value is set to null, false otherwise.
     * @throws ObjectRefDestroyedException If the Value has been destroyed.
     */
    public void setNull(boolean flag) throws ObjectRefDestroyedException {
        checkNotDestroyed();
        Native.kuzu_value_set_null(this, flag);
    }

    /**
     * Copy the Value from another Value.
     *
     * @param other: The Value to copy from.
     * @throws ObjectRefDestroyedException If the Value has been destroyed.
     */
    public void copy(Value other) throws ObjectRefDestroyedException {
        checkNotDestroyed();
        Native.kuzu_value_copy(this, other);
    }

    /**
     * Clone the Value.
     *
     * @return The cloned Value.
     */
    public Value clone() {
        if (destroyed)
            return null;
        else
            return Native.kuzu_value_clone(this);
    }

    /**
     * Get the actual value from the Value.
     *
     * @return The value of the given type.
     * @throws ObjectRefDestroyedException If the Value has been destroyed.
     */
    public <T> T getValue() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_value_get_value(this);
    }

    /**
     * Get the data type of the Value.
     *
     * @return The data type of the Value.
     * @throws ObjectRefDestroyedException If the Value has been destroyed.
     */
    public DataType getDataType() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native.kuzu_value_get_data_type(this);
    }

    /**
     * Convert the Value to string.
     *
     * @return The current value in string format.
     */
    public String toString() {
        if (destroyed)
            return "Value has been destroyed.";
        else
            return Native.kuzu_value_to_string(this);
    }
}
