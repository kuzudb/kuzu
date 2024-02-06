package com.kuzudb;

/**
 * Utility functions for KuzuValue of RDF_VARIANT type.
 */
public class KuzuValueRdfVariantUtil {
    /**
     * Get the data type of the RDF_VARIANT value.
     * @param value: The RDF_VARIANT value.
     * @return The data type of the RDF_VARIANT value.
     * @throws KuzuObjectRefDestroyedException If the RDF_VARIANT value has been destroyed.
     */
    public static KuzuDataType getDataType(KuzuValue value) throws KuzuObjectRefDestroyedException {
       value.checkNotDestroyed();
        return KuzuNative.kuzu_rdf_variant_get_data_type(value);
    }

    /**
     * Get the value of the RDF_VARIANT value.
     * @param value: The RDF_VARIANT value.
     * @return The value of the RDF_VARIANT value.
     * @throws KuzuObjectRefDestroyedException If the RDF_VARIANT value has been destroyed.
     */
    public static <T> T getValue(KuzuValue value) throws KuzuObjectRefDestroyedException {
        value.checkNotDestroyed();
        return KuzuNative.kuzu_rdf_variant_get_value(value);
    }
}
