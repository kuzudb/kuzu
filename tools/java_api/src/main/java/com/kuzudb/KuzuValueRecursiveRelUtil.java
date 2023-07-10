package com.kuzudb;

/**
 * Utility functions for KuzuValue of recursive_rel type.
 */
public class KuzuValueRecursiveRelUtil {
    /**
     * Get the node list from the given recursive_rel value.
     * @param value: The recursive_rel value.
     * @return The node list from the given recursive_rel value.
     * @throws KuzuObjectRefDestroyedException If the recursive_rel value has been destroyed.
     */
    public static KuzuValue getNodeList(KuzuValue value) throws KuzuObjectRefDestroyedException {
        return KuzuValueStructUtil.getValueByIndex(value, 0);
    }

    /**
     * Get the rel list from the given recursive_rel value.
     * @param value: The recursive_rel value.
     * @return The rel list from the given recursive_rel value.
     * @throws KuzuObjectRefDestroyedException If the recursive_rel value has been destroyed.
     */
    public static KuzuValue getRelList(KuzuValue value) throws KuzuObjectRefDestroyedException {
        return KuzuValueStructUtil.getValueByIndex(value, 1);
    }
}
