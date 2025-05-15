package com.kuzudb;

/**
 * FlatTuple stores a vector of values.
 */
public class FlatTuple implements AutoCloseable {
    long ft_ref;
    boolean destroyed = false;

    /**
     * Check if the flat tuple has been destroyed.
     *
     * @throws RuntimeException If the flat tuple has been destroyed.
     */
    private void checkNotDestroyed() {
        if (destroyed)
            throw new RuntimeException("FlatTuple has been destroyed.");
    }

    /**
     * Close the flat tuple and release the underlying resources. This method is invoked automatically on objects managed by the try-with-resources statement.
     *
     * @throws RuntimeException If the flat tuple has been destroyed.
     */
    @Override
    public void close() {
        destroy();
    }

    /**
     * Destroy the flat tuple.
     *
     * @throws RuntimeException If the flat tuple has been destroyed.
     */
    private void destroy() {
        checkNotDestroyed();
        Native.kuzu_flat_tuple_destroy(this);
        destroyed = true;
    }

    /**
     * Get the value at the given index.
     *
     * @param index: The index of the value.
     * @return The value at the given index.
     * @throws RuntimeException If the flat tuple has been destroyed.
     */
    public Value getValue(long index) {
        checkNotDestroyed();
        return Native.kuzu_flat_tuple_get_value(this, index);
    }

    /**
     * Convert the flat tuple to string.
     *
     * @return The string representation of the flat tuple.
     */
    public String toString() {
        if (destroyed)
            return null;
        else
            return Native.kuzu_flat_tuple_to_string(this);
    }
}
