package tools.java_api;

public class KuzuDataType {
    long dt_ref;
    boolean destoryed = false;

    private void checkNotDestoryed () {
        assert !destoryed: "DataType has been destoryed.";
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

    public KuzuDataTypeID getID () {
        checkNotDestoryed();
        return KuzuNative.kuzu_data_type_get_id(this);
    }
}
