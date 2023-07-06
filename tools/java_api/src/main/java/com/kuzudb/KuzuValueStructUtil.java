package com.kuzudb;

public class KuzuValueStructUtil {
    public static long getNumFields(KuzuValue value) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_value_get_list_size(value);
    }

    public static long getIndexByFieldName(KuzuValue value, String fieldName) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_value_get_struct_index(value, fieldName);
    }

    public static String getFieldNameByIndex(KuzuValue value, long index) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_value_get_struct_field_name(value, index);
    }

    public static KuzuValue getValueByFieldName(KuzuValue value, String fieldName) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        long index = getIndexByFieldName(value, fieldName);
        if (index < 0) {
            return null;
        }
        return getValueByIndex(value, index);
    }

    public static KuzuValue getValueByIndex(KuzuValue value, long index) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        if (index < 0 || index >= getNumFields(value)) {
            return null;
        }
        return KuzuNative.kuzu_value_get_list_element(value, index);
    }
}
