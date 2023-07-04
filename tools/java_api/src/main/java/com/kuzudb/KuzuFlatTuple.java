package com.kuzudb;

public class KuzuFlatTuple {
    long ft_ref;
    boolean destroyed = false;

    private void checkNotDestroyed() throws KuzuObjectRefDestroyedException {
        if (destroyed)
            throw new KuzuObjectRefDestroyedException("KuzuFlatTuple has been destroyed.");
    }

    @Override
    protected void finalize() throws KuzuObjectRefDestroyedException {
        destroy();
    }

    public void destroy() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_flat_tuple_destroy(this);
        destroyed = true;
    }

    public KuzuValue getValue(long index) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_flat_tuple_get_value(this, index);
    }

    public String toString() {
        if (destroyed)
            return "KuzuFlatTuple has been destroyed.";
        else
            return KuzuNative.kuzu_flat_tuple_to_string(this);
    }
}
