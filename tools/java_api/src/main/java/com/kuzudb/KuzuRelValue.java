package com.kuzudb;

public class KuzuRelValue {
    public static KuzuInternalID getSrcID(KuzuValue value) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_rel_val_get_src_id(value);
    }

    public static KuzuInternalID getDstID(KuzuValue value) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_rel_val_get_dst_id(value);
    }

    public static String getLabelName(KuzuValue value) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_rel_val_get_label_name(value);
    }

    public static long getPropertySize(KuzuValue value) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_rel_val_get_property_size(value);
    }

    public static String getPropertyNameAt(KuzuValue value, long index) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_rel_val_get_property_name_at(value, index);
    }

    public static KuzuValue getPropertyValueAt(KuzuValue value, long index) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_rel_val_get_property_value_at(value, index);
    }

    public static String toString(KuzuValue value) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_rel_val_to_string(value);
    }
}
