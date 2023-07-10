package com.kuzudb;

/**
 * Utility functions for KuzuValue of rel type.
 */
public class KuzuValueRelUtil {
    /**
     * Get src id of the given rel value.
     * @param value: The rel value.
     * @return The src id of the given rel value.
     * @throws KuzuObjectRefDestroyedException If the rel value has been destroyed.
     */
    public static KuzuInternalID getSrcID(KuzuValue value) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_rel_val_get_src_id(value);
    }

    /**
     * Get dst id of the given rel value.
     * @param value: The rel value.
     * @return The dst id of the given rel value.
     * @throws KuzuObjectRefDestroyedException If the rel value has been destroyed.
     */
    public static KuzuInternalID getDstID(KuzuValue value) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_rel_val_get_dst_id(value);
    }

    /**
     * Get the label name of the rel value.
     * @param value: The rel value.
     * @return The label name of the rel value.
     * @throws KuzuObjectRefDestroyedException If the rel value has been destroyed.
     */
    public static String getLabelName(KuzuValue value) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_rel_val_get_label_name(value);
    }

    /**
     * Get the property size of the rel value.
     * @param value: The rel value.
     * @return The property size of the rel value.
     * @throws KuzuObjectRefDestroyedException If the rel value has been destroyed.
     */
    public static long getPropertySize(KuzuValue value) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_rel_val_get_property_size(value);
    }

    /**
     * Get the property name at the given index from the given rel value.
     * @param value: The rel value.
     * @param index: The index of the property.
     * @return The property name at the given index from the given rel value.
     * @throws KuzuObjectRefDestroyedException If the rel value has been destroyed.
     */
    public static String getPropertyNameAt(KuzuValue value, long index) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_rel_val_get_property_name_at(value, index);
    }

    /**
     * Get the property value at the given index from the given rel value.
     * @param value: The rel value.
     * @param index: The index of the property.
     * @return The property value at the given index from the given rel value.
     * @throws KuzuObjectRefDestroyedException If the rel value has been destroyed.
     */
    public static KuzuValue getPropertyValueAt(KuzuValue value, long index) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_rel_val_get_property_value_at(value, index);
    }

    /**
     * Convert the given rel value to string.
     * @param value: The rel value.
     * @return The given rel value in string format.
     * @throws KuzuObjectRefDestroyedException If the rel value has been destroyed.
     */
    public static String toString(KuzuValue value) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_rel_val_to_string(value);
    }
}
