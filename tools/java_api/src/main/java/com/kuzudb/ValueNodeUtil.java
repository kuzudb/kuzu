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
     * @throws RuntimeException If the node value has been destroyed.
     */
    public static InternalID getID(Value value) {
        value.checkNotDestroyed();
        return Native.kuzuNodeValGetId(value);
    }

    /**
     * Get the label name of the node value.
     *
     * @param value: The node value.
     * @return The label name of the node value.
     * @throws RuntimeException If the node value has been destroyed.
     */
    public static String getLabelName(Value value) {
        value.checkNotDestroyed();
        return Native.kuzuNodeValGetLabelName(value);
    }

    /**
     * Get the property size of the node value.
     *
     * @param value: The node value.
     * @return The property size of the node value.
     * @throws RuntimeException If the node value has been destroyed.
     */
    public static long getPropertySize(Value value) {
        value.checkNotDestroyed();
        return Native.kuzuNodeValGetPropertySize(value);
    }

    /**
     * Get the property name at the given index from the given node value.
     *
     * @param value: The node value.
     * @param index: The index of the property.
     * @return The property name at the given index from the given node value.
     * @throws RuntimeException If the node value has been destroyed.
     */
    public static String getPropertyNameAt(Value value, long index) {
        value.checkNotDestroyed();
        return Native.kuzuNodeValGetPropertyNameAt(value, index);
    }

    /**
     * Get the property value at the given index from the given node value.
     *
     * @param value: The node value.
     * @param index: The index of the property.
     * @return The property value at the given index from the given node value.
     * @throws RuntimeException If the node value has been destroyed.
     */
    public static Value getPropertyValueAt(Value value, long index) {
        value.checkNotDestroyed();
        return Native.kuzuNodeValGetPropertyValueAt(value, index);
    }

    /**
     * Convert the node value to string.
     *
     * @param value: The node value.
     * @return The node value in string format.
     * @throws RuntimeException If the node value has been destroyed.
     */
    public static String toString(Value value) {
        value.checkNotDestroyed();
        return Native.kuzuNodeValToString(value);
    }
}
