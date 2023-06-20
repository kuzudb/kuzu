package tools.java_api;

public class KuzuNodeValue {
    long nv_ref;
    boolean destroyed = false;
    boolean isOwnedByCPP = false;

    private void checkNotDestroyed () throws KuzuObjectRefDestroyedException {
        if (destroyed)
            throw new KuzuObjectRefDestroyedException("KuzuNodeValue has been destroyed.");
    }

    @Override  
    protected void finalize() throws KuzuObjectRefDestroyedException {
        destroy();   
    } 

    public KuzuNodeValue(KuzuInternalID id, String label) {
        nv_ref = KuzuNative.kuzu_node_val_create(id, label);
    }

    public KuzuNodeValue clone() {
        if (destroyed)
            return null;
        else
            return KuzuNative.kuzu_node_val_clone(this);
    }

    public void destroy() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        if (!isOwnedByCPP) {
            KuzuNative.kuzu_node_val_destroy(this);
            destroyed = true;
        }
    }

    public KuzuValue getIDVal() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_node_val_get_id_val(this);
    }

    public KuzuValue getLabelVal() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_node_val_get_label_val(this);
    }

    public KuzuInternalID getID() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_node_val_get_id(this);
    }

    public String getLabelName() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_node_val_get_label_name(this);
    }

    public long getPropertySize() throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_node_val_get_property_size(this);
    }

    public String getPropertyNameAt(long index) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_node_val_get_property_name_at(this, index);
    }

    public KuzuValue getPropertyValueAt(long index) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        return KuzuNative.kuzu_node_val_get_property_value_at(this, index);
    }

    public void addProperty(String key, KuzuValue value) throws KuzuObjectRefDestroyedException {
        checkNotDestroyed();
        KuzuNative.kuzu_node_val_add_property(this, key, value);
    }

    public String toString() {
        if (destroyed)
            return "KuzuNodeValue has been destroyed.";
        else
            return KuzuNative.kuzu_node_val_to_string(this);
    }
}
