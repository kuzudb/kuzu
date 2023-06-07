package tools.java_api;

public class KuzuRelValue {
    long rv_ref;
    boolean destroyed = false;
    boolean isOwnedByCPP = false;

    private void checkNotdestroyed() {
        assert !destroyed: "KuzuRelValue has been destoryed.";
    }

    public KuzuRelValue(KuzuInternalID src_id, KuzuInternalID dst_id, String label) {
        rv_ref = KuzuNative.kuzu_rel_val_create(src_id, dst_id, label);
    }

    public KuzuRelValue clone() {
        checkNotdestroyed();
        return KuzuNative.kuzu_rel_val_clone(this);
    }

    public void destroy() {
        checkNotdestroyed();
        if (!isOwnedByCPP) {
            KuzuNative.kuzu_rel_val_destroy(this);
            destroyed = true;
        }
    }

    public KuzuValue getSrcIDVal() {
        checkNotdestroyed();
        return KuzuNative.kuzu_rel_val_get_src_id_val(this);
    }

    public KuzuValue getDstIDVal() {
        checkNotdestroyed();
        return KuzuNative.kuzu_rel_val_get_dst_id_val(this);
    }

    public KuzuInternalID getSrcID() {
        checkNotdestroyed();
        return KuzuNative.kuzu_rel_val_get_src_id(this);
    }

    public KuzuInternalID getDstID() {
        checkNotdestroyed();
        return KuzuNative.kuzu_rel_val_get_dst_id(this);
    }

    public String getLabelName() {
        checkNotdestroyed();
        return KuzuNative.kuzu_rel_val_get_label_name(this);
    }

    public long getPropertySize() {
        checkNotdestroyed();
        return KuzuNative.kuzu_rel_val_get_property_size(this);
    }

    public String getPropertyNameAt(long index) {
        checkNotdestroyed();
        return KuzuNative.kuzu_rel_val_get_property_name_at(this, index);
    }

    public KuzuValue getPropertyValueAt(long index) {
        checkNotdestroyed();
        return KuzuNative.kuzu_rel_val_get_property_value_at(this, index);
    }

    public void addProperty(String key, KuzuValue value) {
        checkNotdestroyed();
        KuzuNative.kuzu_rel_val_add_property(this, key, value);
    }

    public String toString() {
        checkNotdestroyed();
        return KuzuNative.kuzu_rel_val_to_string(this);
    }
}
