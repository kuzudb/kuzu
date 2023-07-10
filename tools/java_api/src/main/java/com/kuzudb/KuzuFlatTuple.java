package com.kuzudb;

/**
* FlatTuple stores a vector of values.
*/
public class KuzuFlatTuple {
    long ft_ref;
    boolean destroyed = false;

    /**
     * Check if the flat tuple has been destroyed.
     * @throws KuzuObjectRefDestroyedException If the flat tuple has been destroyed.
     */
    private void checkNotDestroyed() throws KuzuObjectRefDestroyedException {
        if (destroyed)
            throw new KuzuObjectRefDestroyedException("KuzuFlatTuple has been destroyed.");
    }

    /**
     * Finalize.
     * @throws KuzuObjectRefDestroyedException If the flat tuple has been destroyed.
     */
    @Override
    protected void finalize() throws KuzuObjectRefDestroyedException {
        destroy();
    }

    /**
     * Destroy the flat tuple.
     * @throws KuzuObjectRefDestroyedException If the flat tuple has been destroyed.
     */
    public void destroy() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_flat_tuple_destroy(this);
        destroyed = true;
    }

    /**
     * Get the value at the given index.
     * @param index: The index of the value.
     * @return The value at the given index.
     * @throws KuzuObjectRefDestroyedException If the flat tuple has been destroyed.
     */
    public KuzuValue getValue(long index) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_flat_tuple_get_value(this, index);
    }

    /**
     * Convert the flat tuple to string.
     * @return The string representation of the flat tuple.
     */
    public String toString() {
        if (destroyed)
            return null;
        else
            return KuzuNative.kuzu_flat_tuple_to_string(this);
    }
}
