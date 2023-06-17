package tools.java_api;

public class KuzuValue {
    long v_ref;
    boolean destroyed = false;
    boolean isOwnedByCPP = false;

    private void checkNotdestroyed () {
        assert !destroyed: "KuzuValue has been destroyed.";
    }

    public boolean isOwnedByCPP() {
        return isOwnedByCPP;
    }

    public <T> KuzuValue (T val) {
        checkNotdestroyed();
        v_ref = KuzuNative.kuzu_value_create_value(val);
    }
    
    public void destroy () {
        checkNotdestroyed();
        if (!isOwnedByCPP) {
            KuzuNative.kuzu_value_destroy(this);
            destroyed = true;
        }
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

    public boolean isNull () {
        checkNotdestroyed();
        return KuzuNative.kuzu_value_is_null(this);
    }

    public void setNull (boolean flag) {
        checkNotdestroyed();
        KuzuNative.kuzu_value_set_null(this, flag);
    }

    public void copy (KuzuValue other) {
        checkNotdestroyed();
        KuzuNative.kuzu_value_copy(this, other);
    }

    public KuzuValue clone () {
        checkNotdestroyed();
        return KuzuNative.kuzu_value_clone(this);
    }

    public <T> T getValue (){
        checkNotdestroyed();
        return KuzuNative.kuzu_value_get_value(this);
    }

    public long getListSize () {
        checkNotdestroyed();
        return KuzuNative.kuzu_value_get_list_size(this);
    }

    public KuzuValue getListElement (long index) {
        checkNotdestroyed();
        return KuzuNative.kuzu_value_get_list_element(this, index);
    }

    public KuzuDataType getDataType () {
        checkNotdestroyed();
        return KuzuNative.kuzu_value_get_data_type(this);
    }

    public String toString () {
        checkNotdestroyed();
        return KuzuNative.kuzu_value_to_string(this);
    }
}
