package com.kuzudb;

/**
 * Utility functions for Value of RDF_VARIANT type.
 */
public class ValueRdfVariantUtil {
    /**
     * Get the data type of the RDF_VARIANT value.
     * @param value: The RDF_VARIANT value.
     * @return The data type of the RDF_VARIANT value.
     * @throws ObjectRefDestroyedException If the RDF_VARIANT value has been destroyed.
     */
    public static DataType getDataType(Value value) throws ObjectRefDestroyedException {
       value.checkNotDestroyed();
        return Native.kuzu_rdf_variant_get_data_type(value);
    }

    /**
     * Get the value of the RDF_VARIANT value.
     * @param value: The RDF_VARIANT value.
     * @return The value of the RDF_VARIANT value.
     * @throws ObjectRefDestroyedException If the RDF_VARIANT value has been destroyed.
     */
    public static <T> T getValue(Value value) throws ObjectRefDestroyedException {
        value.checkNotDestroyed();
        return Native.kuzu_rdf_variant_get_value(value);
    }
}
