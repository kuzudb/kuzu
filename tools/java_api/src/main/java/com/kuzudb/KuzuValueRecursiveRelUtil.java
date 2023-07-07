package com.kuzudb;

public class KuzuValueRecursiveRelUtil {
    public static KuzuValue getNodeList(KuzuValue value) throws KuzuObjectRefDestroyedException {
        return KuzuValueStructUtil.getValueByIndex(value, 0);
    }

    public static KuzuValue getRelList(KuzuValue value) throws KuzuObjectRefDestroyedException {
        return KuzuValueStructUtil.getValueByIndex(value, 1);
    }
}
