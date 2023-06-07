package tools.java_api;

public class KuzuNodeValue {
    long nv_ref;
    boolean destroyed = false;
    boolean isOwnedByCPP = false;

    private void checkNotdestroyed() {
        assert !destroyed: "KuzuNodeValue has been destoryed.";
    }

    public KuzuNodeValue(KuzuInternalID id, String label) {
        nv_ref = KuzuNative.kuzu_node_val_create(id, label);
    }

    public KuzuNodeValue clone() {
        checkNotdestroyed();
        return KuzuNative.kuzu_node_val_clone(this);
    }

    public void destroy() {
        checkNotdestroyed();
        if (!isOwnedByCPP) {
            KuzuNative.kuzu_node_val_destroy(this);
            destroyed = true;
        }
    }

    public KuzuValue getIDVal() {
        checkNotdestroyed();
        return KuzuNative.kuzu_node_val_get_id_val(this);
    }

    public KuzuValue getLabelVal() {
        checkNotdestroyed();
        return KuzuNative.kuzu_node_val_get_label_val(this);
    }

    public KuzuInternalID getID() {
        checkNotdestroyed();
        return KuzuNative.kuzu_node_val_get_id(this);
    }

    public String getLabelName() {
        checkNotdestroyed();
        return KuzuNative.kuzu_node_val_get_label_name(this);
    }

    public long getPropertySize() {
        checkNotdestroyed();
        return KuzuNative.kuzu_node_val_get_property_size(this);
    }

    public String getPropertyNameAt(long index) {
        checkNotdestroyed();
        return KuzuNative.kuzu_node_val_get_property_name_at(this, index);
    }

    public KuzuValue getPropertyValueAt(long index) {
        checkNotdestroyed();
        return KuzuNative.kuzu_node_val_get_property_value_at(this, index);
    }

    public void addProperty(String key, KuzuValue value) {
        checkNotdestroyed();
        KuzuNative.kuzu_node_val_add_property(this, key, value);
    }

    public String toString() {
        checkNotdestroyed();
        return KuzuNative.kuzu_node_val_to_string(this);
    }
}
