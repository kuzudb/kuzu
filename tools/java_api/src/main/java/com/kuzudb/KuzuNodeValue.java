package com.kuzudb;

public class KuzuNodeValue {
    public static KuzuInternalID getID(KuzuValue value) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_node_val_get_id(value);
    }

    public static String getLabelName(KuzuValue value) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_node_val_get_label_name(value);
    }

    public static long getPropertySize(KuzuValue value) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_node_val_get_property_size(value);
    }

    public static String getPropertyNameAt(KuzuValue value, long index) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_node_val_get_property_name_at(value, index);
    }

    public static KuzuValue getPropertyValueAt(KuzuValue value, long index) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_node_val_get_property_value_at(value, index);
    }

    public static String toString(KuzuValue value) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_node_val_to_string(value);
    }
}
