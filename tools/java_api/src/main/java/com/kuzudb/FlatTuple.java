package com.kuzudb;

/**
* FlatTuple stores a vector of values.
*/
public class FlatTuple implements AutoCloseable {
    long ft_ref;
    boolean destroyed = false;

    /**
     * Check if the flat tuple has been destroyed.
     * @throws ObjectRefDestroyedException If the flat tuple has been destroyed.
     */
    private void checkNotDestroyed() throws ObjectRefDestroyedException {
        if (destroyed)
            throw new ObjectRefDestroyedException("FlatTuple has been destroyed.");
    }

    /**
    * Close the flat tuple and release the underlying resources. This method is invoked automatically on objects managed by the try-with-resources statement.
     * @throws ObjectRefDestroyedException If the flat tuple has been destroyed.
     */
    @Override
    public void close() throws ObjectRefDestroyedException {
        destroy();
    }

    /**
     * Destroy the flat tuple.
     * @throws ObjectRefDestroyedException If the flat tuple has been destroyed.
     */
    private void destroy() throws ObjectRefDestroyedException {
        checkNotDestroyed();
        Native.kuzu_flat_tuple_destroy(this);
        destroyed = true;
    }

    /**
     * Get the value at the given index.
     * @param index: The index of the value.
     * @return The value at the given index.
     * @throws ObjectRefDestroyedException If the flat tuple has been destroyed.
     */
    public Value getValue(long index) throws ObjectRefDestroyedException {
        checkNotDestroyed();
        return Native. kuzu_flat_tuple_get_value(this, index);
    }

    /**
     * Convert the flat tuple to string.
     * @return The string representation of the flat tuple.
     */
    public String toString() {
        if (destroyed)
            return null;
        else
            return Native.kuzu_flat_tuple_to_string(this);
    }
}
