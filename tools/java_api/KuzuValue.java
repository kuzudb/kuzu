package tools.java_api;

public class KuzuValue {
    long v_ref;
    boolean destroyed = false;

    public <T> KuzuValue (T val) {
        v_ref = KuzuNative.kuzu_value_create_value(val);
    }
    public void destroy () {
        KuzuNative.kuzu_value_destroy(this);
        destroyed = true;
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
        return KuzuNative.kuzu_value_is_null(this);
    }

    public void setNull (boolean flag) {
        KuzuNative.kuzu_value_set_null(this, flag);
    }

    public void copy (KuzuValue other) {
        KuzuNative.kuzu_value_copy(this, other);
    }

    public KuzuValue clone () {
        return KuzuNative.kuzu_value_clone(this);
    }

    public <T> T getValue (){
        return KuzuNative.kuzu_value_get_value(this);
    }

    public long getListSize () {
        return KuzuNative.kuzu_value_get_list_size(this);
    }

    public KuzuValue getListElement (long index) {
        return KuzuNative.kuzu_value_get_list_element(this, index);
    }

    public KuzuDataType getDataType () {
        return KuzuNative.kuzu_value_get_data_type(this);
    }

    public String toString () {
        return KuzuNative.kuzu_value_to_string(this);
    }
}
