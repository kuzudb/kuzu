package com.kuzudb;

/**
 * Utility functions for Value of recursive_rel type.
 */
public class ValueRecursiveRelUtil {
    /**
     * Get the node list from the given recursive_rel value.
     *
     * @param value: The recursive_rel value.
     * @return The node list from the given recursive_rel value.
     * @throws RuntimeException If the recursive_rel value has been
     *                                     destroyed.
     */
    public static Value getNodeList(Value value) {
        return new KuzuStruct(value).getValueByIndex(0);
    }

    /**
     * Get the rel list from the given recursive_rel value.
     *
     * @param value: The recursive_rel value.
     * @return The rel list from the given recursive_rel value.
     * @throws RuntimeException If the recursive_rel value has been
     *                                     destroyed.
     */
    public static Value getRelList(Value value) {
        return new KuzuStruct(value).getValueByIndex(1);
    }
}
