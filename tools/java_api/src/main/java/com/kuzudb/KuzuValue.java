package com.kuzudb;

public class KuzuValue {
    long v_ref;
    boolean destroyed = false;
    boolean isOwnedByCPP = false;

    public <T> KuzuValue(T val) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        v_ref = KuzuNative.kuzu_value_create_value(val);
    }

    public static KuzuValue createNull() {
        return KuzuNative.kuzu_value_create_null();
    }

    public static KuzuValue createNullWithDataType(KuzuDataType data_type) {
        return KuzuNative.kuzu_value_create_null_with_data_type(data_type);
    }

    public static KuzuValue createDefault(KuzuDataType data_type) {
        return KuzuNative.kuzu_value_create_default(data_type);
    }

    public void checkNotDestroyed() throws KuzuObjectRefDestroyedException {
        if (destroyed)
            throw new KuzuObjectRefDestroyedException("KuzuValue has been destroyed.");
    }

    @Override
    protected void finalize() throws KuzuObjectRefDestroyedException {
        destroy();
    }

    public boolean isOwnedByCPP() {
        return isOwnedByCPP;
    }

    public void destroy() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        if (!isOwnedByCPP) {
            KuzuNative.kuzu_value_destroy(this);
            destroyed = true;
        }
    }

    public boolean isNull() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_value_is_null(this);
    }

    public void setNull(boolean flag) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_value_set_null(this, flag);
    }

    public void copy(KuzuValue other) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_value_copy(this, other);
    }

    public KuzuValue clone() {
        if (destroyed)
            return null;
        else
            return KuzuNative.kuzu_value_clone(this);
    }

    public <T> T getValue() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_value_get_value(this);
    }

    public KuzuDataType getDataType() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_value_get_data_type(this);
    }

    public String toString() {
        if (destroyed)
            return "KuzuValue has been destroyed.";
        else
            return KuzuNative.kuzu_value_to_string(this);
    }
}
