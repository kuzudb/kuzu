package com.kuzudb;

public class KuzuRelValue {
    long rv_ref;
    boolean destroyed = false;
    boolean isOwnedByCPP = false;

    public KuzuRelValue(KuzuInternalID src_id, KuzuInternalID dst_id, String label) {
        rv_ref = KuzuNative.kuzu_rel_val_create(src_id, dst_id, label);
    }

    private void checkNotDestroyed() throws KuzuObjectRefDestroyedException {
        if (destroyed)
            throw new KuzuObjectRefDestroyedException("KuzuRelValue has been destroyed.");
    }

    @Override
    protected void finalize() throws KuzuObjectRefDestroyedException {
        destroy();
    }

    public KuzuRelValue clone() {
        if (destroyed)
            return null;
        else
            return KuzuNative.kuzu_rel_val_clone(this);
    }

    public void destroy() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        if (!isOwnedByCPP) {
            KuzuNative.kuzu_rel_val_destroy(this);
            destroyed = true;
        }
    }

    public KuzuValue getSrcIDVal() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_rel_val_get_src_id_val(this);
    }

    public KuzuValue getDstIDVal() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_rel_val_get_dst_id_val(this);
    }

    public KuzuInternalID getSrcID() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_rel_val_get_src_id(this);
    }

    public KuzuInternalID getDstID() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_rel_val_get_dst_id(this);
    }

    public String getLabelName() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_rel_val_get_label_name(this);
    }

    public long getPropertySize() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_rel_val_get_property_size(this);
    }

    public String getPropertyNameAt(long index) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_rel_val_get_property_name_at(this, index);
    }

    public KuzuValue getPropertyValueAt(long index) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_rel_val_get_property_value_at(this, index);
    }

    public void addProperty(String key, KuzuValue value) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_rel_val_add_property(this, key, value);
    }

    public String toString() {
        if (destroyed)
            return "KuzuRelValue has been destroyed.";
        else
            return KuzuNative.kuzu_rel_val_to_string(this);
    }
}
