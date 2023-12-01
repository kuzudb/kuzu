package com.kuzudb;

/**
 * KuzuValue can hold data of different types.
 */
public class KuzuValue {
    long v_ref;
    boolean destroyed = false;
    boolean isOwnedByCPP = false;

    /**
     * Construct a KuzuValue from a val.
     * @throws KuzuObjectRefDestroyedException If the KuzuValue has been destroyed.
     */
    public <T> KuzuValue(T val) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        v_ref = KuzuNative.kuzu_value_create_value(val);
    }

    /**
     * Create a null KuzuValue.
     * @return The null KuzuValue.
     */
    public static KuzuValue createNull() {
        return KuzuNative.kuzu_value_create_null();
    }

    /**
     * Create a null KuzuValue with the given data type.
     * @param data_type: The data type of the null KuzuValue.
     */
    public static KuzuValue createNullWithDataType(KuzuDataType data_type) {
        return KuzuNative.kuzu_value_create_null_with_data_type(data_type);
    }

    /**
     * Create a default KuzuValue with the given data type.
     * @param data_type: The data type of the default KuzuValue.
     * @return The default KuzuValue.
     */
    public static KuzuValue createDefault(KuzuDataType data_type) {
        return KuzuNative.kuzu_value_create_default(data_type);
    }

    /**
     * Check if the KuzuValue has been destroyed.
     * @throws KuzuObjectRefDestroyedException If the KuzuValue has been destroyed.
     */
    public void checkNotDestroyed() throws KuzuObjectRefDestroyedException {
        if (destroyed)
            throw new KuzuObjectRefDestroyedException("KuzuValue has been destroyed.");
    }

    /**
     * Finalize.
     * @throws KuzuObjectRefDestroyedException If the KuzuValue has been destroyed.
     */
    @Override
    protected void finalize() throws KuzuObjectRefDestroyedException {
        destroy();
    }

    public boolean isOwnedByCPP() {
        return isOwnedByCPP;
    }

    /**
     * Destroy the KuzuValue.
     * @throws KuzuObjectRefDestroyedException If the KuzuValue has been destroyed.
     */
    public void destroy() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        if (!isOwnedByCPP) {
            KuzuNative.kuzu_value_destroy(this);
            destroyed = true;
        }
    }

    /**
     * Check if the KuzuValue is null.
     * @return True if the KuzuValue is null, false otherwise.
     * @throws KuzuObjectRefDestroyedException If the KuzuValue has been destroyed.
     */
    public boolean isNull() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_value_is_null(this);
    }

    /**
     * Set the KuzuValue to null.
     * @param flag: True if the KuzuValue is set to null, false otherwise.
     * @throws KuzuObjectRefDestroyedException If the KuzuValue has been destroyed.
     */
    public void setNull(boolean flag) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_value_set_null(this, flag);
    }

    /**
     * Copy the KuzuValue from another KuzuValue.
     * @param other: The KuzuValue to copy from.
     * @throws KuzuObjectRefDestroyedException If the KuzuValue has been destroyed.
     */
    public void copy(KuzuValue other) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_value_copy(this, other);
    }

    /**
     * Clone the KuzuValue.
     * @return The cloned KuzuValue.
     */
    public KuzuValue clone() {
        if (destroyed)
            return null;
        else
            return KuzuNative.kuzu_value_clone(this);
    }

    /**
     * Get the actual value from the KuzuValue.
     * @return The value of the given type.
     * @throws KuzuObjectRefDestroyedException If the KuzuValue has been destroyed.
     */
    public <T> T getValue() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_value_get_value(this);
    }

    /**
     * Get the data type of the KuzuValue.
     * @return The data type of the KuzuValue.
     * @throws KuzuObjectRefDestroyedException If the KuzuValue has been destroyed.
     */
    public KuzuDataType getDataType() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_value_get_data_type(this);
    }

    /**
     * Convert the KuzuValue to string.
     * @return The current value in string format.
     */
    public String toString() {
        if (destroyed)
            return "KuzuValue has been destroyed.";
        else
            return KuzuNative.kuzu_value_to_string(this);
    }
}
