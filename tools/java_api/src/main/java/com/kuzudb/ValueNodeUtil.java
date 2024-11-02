package com.kuzudb;

/**
 * Utility functions for Value of node type.
 */
public class ValueNodeUtil {
    /**
     * Get the internal ID of the node value.
     *
     * @param value: The node value.
     * @return The internal ID of the node value.
     * @throws ObjectRefDestroyedException If the node value has been destroyed.
     */
    public static InternalID getID(Value value) throws ObjectRefDestroyedException {
        value.checkNotDestroyed();
        return Native.kuzu_node_val_get_id(value);
    }

    /**
     * Get the label name of the node value.
     *
     * @param value: The node value.
     * @return The label name of the node value.
     * @throws ObjectRefDestroyedException If the node value has been destroyed.
     */
    public static String getLabelName(Value value) throws ObjectRefDestroyedException {
        value.checkNotDestroyed();
        return Native.kuzu_node_val_get_label_name(value);
    }

    /**
     * Get the property size of the node value.
     *
     * @param value: The node value.
     * @return The property size of the node value.
     * @throws ObjectRefDestroyedException If the node value has been destroyed.
     */
    public static long getPropertySize(Value value) throws ObjectRefDestroyedException {
        value.checkNotDestroyed();
        return Native.kuzu_node_val_get_property_size(value);
    }

    /**
     * Get the property name at the given index from the given node value.
     *
     * @param value: The node value.
     * @param index: The index of the property.
     * @return The property name at the given index from the given node value.
     * @throws ObjectRefDestroyedException If the node value has been destroyed.
     */
    public static String getPropertyNameAt(Value value, long index) throws ObjectRefDestroyedException {
        value.checkNotDestroyed();
        return Native.kuzu_node_val_get_property_name_at(value, index);
    }

    /**
     * Get the property value at the given index from the given node value.
     *
     * @param value: The node value.
     * @param index: The index of the property.
     * @return The property value at the given index from the given node value.
     * @throws ObjectRefDestroyedException If the node value has been destroyed.
     */
    public static Value getPropertyValueAt(Value value, long index) throws ObjectRefDestroyedException {
        value.checkNotDestroyed();
        return Native.kuzu_node_val_get_property_value_at(value, index);
    }

    /**
     * Convert the node value to string.
     *
     * @param value: The node value.
     * @return The node value in string format.
     * @throws ObjectRefDestroyedException If the node value has been destroyed.
     */
    public static String toString(Value value) throws ObjectRefDestroyedException {
        value.checkNotDestroyed();
        return Native.kuzu_node_val_to_string(value);
    }
}
