package com.kuzudb;

public class KuzuDataType {
    long dt_ref;
    boolean destroyed = false;

    public KuzuDataType(KuzuDataTypeID id) {
        dt_ref = KuzuNative.kuzu_data_type_create(id, null, 0);
    }

    public KuzuDataType
            (KuzuDataTypeID id, KuzuDataType child_type, long fixed_num_elements_in_list) {
        dt_ref = KuzuNative.kuzu_data_type_create(id, child_type, fixed_num_elements_in_list);
    }

    private void checkNotDestroyed() throws KuzuObjectRefDestroyedException {
        if (destroyed)
            throw new KuzuObjectRefDestroyedException("KuzuDatabase has been destroyed.");
    }

    @Override
    protected void finalize() throws KuzuObjectRefDestroyedException {
        destroy();
    }

    public void destroy() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_data_type_destroy(this);
        destroyed = true;
    }

    public KuzuDataType clone() {
        if (destroyed)
            return null;
        else
            return KuzuNative.kuzu_data_type_clone(this);
    }

    public boolean equals(KuzuDataType other) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_data_type_equals(this, other);
    }

    public KuzuDataTypeID getID() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_data_type_get_id(this);
    }

    public KuzuDataType getChildType() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_data_type_get_child_type(this);
    }

    public long getFixedNumElementsInList() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_data_type_get_fixed_num_elements_in_list(this);
    }

}
