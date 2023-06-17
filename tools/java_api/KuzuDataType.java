package tools.java_api;

public class KuzuDataType {
    long dt_ref;
    boolean destoryed = false;

    private void checkNotDestoryed () {
        assert !destoryed: "DataType has been destoryed.";
    }

    public void destory() {
        checkNotDestoryed();
        KuzuNative.kuzu_data_type_destroy(this);
        destoryed = true;
    }

    public KuzuDataType (KuzuDataTypeID id) {
        checkNotDestoryed();
        dt_ref = KuzuNative.kuzu_data_type_create(id, null, 0);
    }

    public KuzuDataType 
        (KuzuDataTypeID id, KuzuDataType child_type, long fixed_num_elements_in_list) {
        checkNotDestoryed();
        dt_ref = KuzuNative.kuzu_data_type_create(id, child_type, fixed_num_elements_in_list);
    }

    public KuzuDataType clone() {
        return KuzuNative.kuzu_data_type_clone(this);
    }

    public boolean equals (KuzuDataType other) {
        checkNotDestoryed();
        return KuzuNative.kuzu_data_type_equals(this, other);
    }

    public KuzuDataTypeID getID () {
        checkNotDestoryed();
        return KuzuNative.kuzu_data_type_get_id(this);
    }

    public KuzuDataType getChildType () {
        checkNotDestoryed();
        return KuzuNative.kuzu_data_type_get_child_type(this);
    }

    public long getFixedNumElementsInList () {
        checkNotDestoryed();
        return KuzuNative.kuzu_data_type_get_fixed_num_elements_in_list(this);
    }


}
