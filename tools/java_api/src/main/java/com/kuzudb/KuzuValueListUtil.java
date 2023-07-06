package com.kuzudb;

public class KuzuValueListUtil {
    public static long getListSize(KuzuValue value) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_value_get_list_size(value);
    }

    public static KuzuValue getListElement(KuzuValue value, long index) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_value_get_list_element(value, index);
    }
}
