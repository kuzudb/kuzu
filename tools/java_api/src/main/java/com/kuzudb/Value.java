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
     * @throws RuntimeException If the Value has been destroyed.
     */
    public <T> Value(T val) {
        checkNotDestroyed();
        v_ref = Native.kuzuValueCreateValue(val);
    }

    /**
     * Create a null Value.
     *
     * @return The null Value.
     */
    public static Value createNull() {
        return Native.kuzuValueCreateNull();
    }

    /**
     * Create a null Value with the given data type.
     *
     * @param data_type: The data type of the null Value.
     */
    public static Value createNullWithDataType(DataType data_type) {
        return Native.kuzuValueCreateNullWithDataType(data_type);
    }

    /**
     * Create a default Value with the given data type.
     *
     * @param data_type: The data type of the default Value.
     * @return The default Value.
     */
    public static Value createDefault(DataType data_type) {
        return Native.kuzuValueCreateDefault(data_type);
    }

    /**
     * Check if the Value has been destroyed.
     *
     * @throws RuntimeException If the Value has been destroyed.
     */
    public void checkNotDestroyed() {
        if (destroyed)
            throw new RuntimeException("Value has been destroyed.");
    }

    /**
     * Close the value and release the underlying resources. This method is invoked automatically on objects managed by the try-with-resources statement.
     *
     * @throws RuntimeException If the Value has been destroyed.
     */
    @Override
    public void close() {
        destroy();
    }

    public boolean isOwnedByCPP() {
        return isOwnedByCPP;
    }

    /**
     * Destroy the Value.
     *
     * @throws RuntimeException If the Value has been destroyed.
     */
    private void destroy() {
        checkNotDestroyed();
        if (!isOwnedByCPP) {
            Native.kuzuValueDestroy(this);
            destroyed = true;
        }
    }

    /**
     * Check if the Value is null.
     *
     * @return True if the Value is null, false otherwise.
     * @throws RuntimeException If the Value has been destroyed.
     */
    public boolean isNull() {
        checkNotDestroyed();
        return Native.kuzuValueIsNull(this);
    }

    /**
     * Set the Value to null.
     *
     * @param flag: True if the Value is set to null, false otherwise.
     * @throws RuntimeException If the Value has been destroyed.
     */
    public void setNull(boolean flag) {
        checkNotDestroyed();
        Native.kuzuValueSetNull(this, flag);
    }

    /**
     * Copy the Value from another Value.
     *
     * @param other: The Value to copy from.
     * @throws RuntimeException If the Value has been destroyed.
     */
    public void copy(Value other) {
        checkNotDestroyed();
        Native.kuzuValueCopy(this, other);
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
            return Native.kuzuValueClone(this);
    }

    /**
     * Get the actual value from the Value.
     *
     * @return The value of the given type.
     * @throws RuntimeException If the Value has been destroyed.
     */
    public <T> T getValue() {
        checkNotDestroyed();
        return Native.kuzuValueGetValue(this);
    }

    /**
     * Get the data type of the Value.
     *
     * @return The data type of the Value.
     * @throws RuntimeException If the Value has been destroyed.
     */
    public DataType getDataType() {
        checkNotDestroyed();
        return Native.kuzuValueGetDataType(this);
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
            return Native.kuzuValueToString(this);
    }
}
