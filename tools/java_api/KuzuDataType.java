package tools.java_api;

public class KuzuDataType {
    long dt_ref;

    public KuzuDataType (KuzuDataTypeID id) {
        dt_ref = KuzuNative.kuzu_data_type_create(id, null, 0);
    }

    public KuzuDataType 
        (KuzuDataTypeID id, KuzuDataType child_type, long fixed_num_elements_in_list) {
        dt_ref = KuzuNative.kuzu_data_type_create(id, child_type, fixed_num_elements_in_list);
    }

    public KuzuDataTypeID getID () {
        return KuzuNative.kuzu_data_type_get_id(this);
    }
}
