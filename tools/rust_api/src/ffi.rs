#[allow(clippy::module_inception)]
#[cxx::bridge]
pub(crate) mod ffi {
    // From types.h
    // Note: cxx will check if values change, but not if they are added.
    #[namespace = "kuzu::common"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum LogicalTypeID {
        ANY = 0,
        NODE = 10,
        REL = 11,
        RECURSIVE_REL = 12,
        // SERIAL is a special data type that is used to represent a sequence of INT64 values that are
        // incremented by 1 starting from 0.
        SERIAL = 13,

        // fixed size types
        BOOL = 22,
        INT64 = 23,
        INT32 = 24,
        INT16 = 25,
        DOUBLE = 26,
        FLOAT = 27,
        DATE = 28,
        TIMESTAMP = 29,
        INTERVAL = 30,
        FIXED_LIST = 31,

        INTERNAL_ID = 40,

        ARROW_COLUMN = 41,

        // variable size types
        STRING = 50,
        BLOB = 51,
        VAR_LIST = 52,
        STRUCT = 53,
        MAP = 54,
        UNION = 55,
    }
    #[namespace = "kuzu::common"]
    unsafe extern "C++" {
        type LogicalTypeID;
    }

    #[namespace = "kuzu::main"]
    unsafe extern "C++" {
        include!("kuzu/include/kuzu_rs.h");

        type PreparedStatement;
        fn isSuccess(&self) -> bool;

        #[namespace = "kuzu_rs"]
        fn prepared_statement_error_message(statement: &PreparedStatement) -> String;
    }

    #[namespace = "kuzu_rs"]
    unsafe extern "C++" {
        type QueryParams;

        // Simple types which cross the ffi without problems
        // Non-copyable types are references so that they only need to be cloned on the
        // C++ side of things
        fn insert(self: Pin<&mut Self>, key: &str, value: UniquePtr<Value>);

        fn new_params() -> UniquePtr<QueryParams>;
    }

    #[namespace = "kuzu_rs"]
    unsafe extern "C++" {
        #[namespace = "kuzu::main"]
        type Database;

        fn new_database(
            databasePath: &CxxString,
            bufferPoolSize: u64,
        ) -> Result<UniquePtr<Database>>;

        fn database_set_logging_level(database: Pin<&mut Database>, level: &CxxString);
    }

    #[namespace = "kuzu::main"]
    unsafe extern "C++" {
        // The C++ Connection class includes a pointer to the database.
        // We must not destroy a referenced database while a connection is open.
        type Connection<'a>;

        #[namespace = "kuzu_rs"]
        fn database_connect<'a>(
            database: Pin<&'a mut Database>,
        ) -> Result<UniquePtr<Connection<'a>>>;

        fn prepare(
            self: Pin<&mut Connection>,
            query: &CxxString,
        ) -> Result<UniquePtr<PreparedStatement>>;

        #[namespace = "kuzu_rs"]
        fn connection_execute(
            connection: Pin<&mut Connection>,
            query: Pin<&mut PreparedStatement>,
            params: UniquePtr<QueryParams>,
        ) -> Result<UniquePtr<QueryResult>>;

        fn getMaxNumThreadForExec(self: Pin<&mut Connection>) -> u64;
        fn setMaxNumThreadForExec(self: Pin<&mut Connection>, num_threads: u64);
        fn beginReadOnlyTransaction(self: Pin<&mut Connection>) -> Result<()>;
        fn beginWriteTransaction(self: Pin<&mut Connection>) -> Result<()>;
        fn commit(self: Pin<&mut Connection>) -> Result<()>;
        fn rollback(self: Pin<&mut Connection>) -> Result<()>;
        fn interrupt(self: Pin<&mut Connection>) -> Result<()>;
        fn setQueryTimeOut(self: Pin<&mut Connection>, timeout_ms: u64);

        #[namespace = "kuzu_rs"]
        fn get_node_table_names(conn: Pin<&mut Connection>) -> String;
        #[namespace = "kuzu_rs"]
        fn get_rel_table_names(conn: Pin<&mut Connection>) -> String;
        #[namespace = "kuzu_rs"]
        fn get_node_property_names(conn: Pin<&mut Connection>, node_table_name: &str) -> String;
        #[namespace = "kuzu_rs"]
        fn get_rel_property_names(conn: Pin<&mut Connection>, rel_table_name: &str) -> String;
    }

    #[namespace = "kuzu::main"]
    unsafe extern "C++" {
        type QueryResult;

        #[namespace = "kuzu_rs"]
        fn query_result_to_string(query_result: Pin<&mut QueryResult>) -> String;
        fn isSuccess(&self) -> bool;
        #[namespace = "kuzu_rs"]
        fn query_result_get_error_message(query_result: &QueryResult) -> String;
        fn hasNext(&self) -> bool;
        fn getNext(self: Pin<&mut QueryResult>) -> SharedPtr<FlatTuple>;

        #[namespace = "kuzu_rs"]
        fn query_result_get_compiling_time(result: &QueryResult) -> f64;
        #[namespace = "kuzu_rs"]
        fn query_result_get_execution_time(result: &QueryResult) -> f64;
        fn getNumColumns(&self) -> usize;
        fn getNumTuples(&self) -> u64;
        #[namespace = "kuzu_rs"]
        fn query_result_write_to_csv(
            query_result: Pin<&mut QueryResult>,
            filename: &String,
            delimiter: i8,
            escape_character: i8,
            newline: i8,
        ) -> Result<()>;

        #[namespace = "kuzu_rs"]
        fn query_result_column_data_types(
            query_result: &QueryResult,
        ) -> UniquePtr<CxxVector<LogicalType>>;
        #[namespace = "kuzu_rs"]
        fn query_result_column_names(query_result: &QueryResult) -> Vec<String>;
    }

    #[namespace = "kuzu::processor"]
    unsafe extern "C++" {
        type FlatTuple;

        fn len(&self) -> u32;
        #[namespace = "kuzu_rs"]
        fn flat_tuple_get_value(tuple: &FlatTuple, index: u32) -> &Value;
    }

    #[namespace = "kuzu_rs"]
    unsafe extern "C++" {
        type ValueList<'a>;

        fn size<'a>(&'a self) -> u64;
        fn get<'a>(&'a self, index: u64) -> &'a UniquePtr<Value>;
    }

    #[namespace = "kuzu_rs"]
    unsafe extern "C++" {
        #[namespace = "kuzu::common"]
        type LogicalType;

        #[namespace = "kuzu::common"]
        fn getLogicalTypeID(&self) -> LogicalTypeID;

        fn create_logical_type(id: LogicalTypeID) -> UniquePtr<LogicalType>;
        fn create_logical_type_var_list(
            child_type: UniquePtr<LogicalType>,
        ) -> UniquePtr<LogicalType>;
        fn create_logical_type_fixed_list(
            child_type: UniquePtr<LogicalType>,
            num_elements: u64,
        ) -> UniquePtr<LogicalType>;
        fn create_logical_type_struct(
            field_names: &Vec<String>,
            types: UniquePtr<TypeListBuilder>,
        ) -> UniquePtr<LogicalType>;

        fn logical_type_get_var_list_child_type(value: &LogicalType) -> &LogicalType;
        fn logical_type_get_fixed_list_child_type(value: &LogicalType) -> &LogicalType;
        fn logical_type_get_fixed_list_num_elements(value: &LogicalType) -> u64;
        fn logical_type_get_struct_field_names(value: &LogicalType) -> Vec<String>;
        fn logical_type_get_struct_field_types(
            value: &LogicalType,
        ) -> UniquePtr<CxxVector<LogicalType>>;
    }

    #[namespace = "kuzu_rs"]
    unsafe extern "C++" {
        type ValueListBuilder;

        fn insert(self: Pin<&mut ValueListBuilder>, value: UniquePtr<Value>);
        fn get_list_value(
            typ: UniquePtr<LogicalType>,
            value: UniquePtr<ValueListBuilder>,
        ) -> UniquePtr<Value>;
        fn create_list() -> UniquePtr<ValueListBuilder>;
    }

    #[namespace = "kuzu_rs"]
    unsafe extern "C++" {
        type TypeListBuilder;

        fn insert(self: Pin<&mut TypeListBuilder>, typ: UniquePtr<LogicalType>);
        fn create_type_list() -> UniquePtr<TypeListBuilder>;
    }

    #[namespace = "kuzu_rs"]
    unsafe extern "C++" {
        #[namespace = "kuzu::common"]
        type Value;

        // only used by tests
        #[allow(dead_code)]
        fn value_to_string(node_value: &Value) -> String;

        #[rust_name = "get_value_bool"]
        fn getValue(&self) -> bool;
        #[rust_name = "get_value_i16"]
        fn getValue(&self) -> i16;
        #[rust_name = "get_value_i32"]
        fn getValue(&self) -> i32;
        #[rust_name = "get_value_i64"]
        fn getValue(&self) -> i64;
        #[rust_name = "get_value_float"]
        fn getValue(&self) -> f32;
        #[rust_name = "get_value_double"]
        fn getValue(&self) -> f64;

        fn value_get_string(value: &Value) -> String;
        #[rust_name = "value_get_node_val"]
        fn value_get_unique(value: &Value) -> UniquePtr<NodeVal>;
        #[rust_name = "value_get_rel_val"]
        fn value_get_unique(value: &Value) -> UniquePtr<RelVal>;
        fn value_get_interval_secs(value: &Value) -> i64;
        fn value_get_interval_micros(value: &Value) -> i32;
        fn value_get_timestamp_micros(value: &Value) -> i64;
        fn value_get_date_days(value: &Value) -> i32;
        fn value_get_internal_id(value: &Value) -> [u64; 2];
        fn value_get_list(value: &Value) -> UniquePtr<ValueList>;

        fn value_get_data_type_id(value: &Value) -> LogicalTypeID;
        fn value_get_data_type(value: &Value) -> UniquePtr<LogicalType>;

        fn isNull(&self) -> bool;

        #[rust_name = "create_value_bool"]
        fn create_value(value: bool) -> UniquePtr<Value>;
        #[rust_name = "create_value_i16"]
        fn create_value(value: i16) -> UniquePtr<Value>;
        #[rust_name = "create_value_i32"]
        fn create_value(value: i32) -> UniquePtr<Value>;
        #[rust_name = "create_value_i64"]
        fn create_value(value: i64) -> UniquePtr<Value>;
        #[rust_name = "create_value_float"]
        fn create_value(value: f32) -> UniquePtr<Value>;
        #[rust_name = "create_value_double"]
        fn create_value(value: f64) -> UniquePtr<Value>;

        fn create_value_null(typ: UniquePtr<LogicalType>) -> UniquePtr<Value>;
        fn create_value_string(value: &String) -> UniquePtr<Value>;
        fn create_value_timestamp(value: i64) -> UniquePtr<Value>;
        fn create_value_date(value: i64) -> UniquePtr<Value>;
        fn create_value_interval(months: i32, days: i32, micros: i64) -> UniquePtr<Value>;
        fn create_value_internal_id(offset: u64, table: u64) -> UniquePtr<Value>;
        fn create_value_node(
            id_val: UniquePtr<Value>,
            label_val: UniquePtr<Value>,
        ) -> UniquePtr<Value>;
        fn create_value_rel(
            src_id: UniquePtr<Value>,
            dst_id: UniquePtr<Value>,
            label_val: UniquePtr<Value>,
        ) -> UniquePtr<Value>;

        fn value_add_property(value: Pin<&mut Value>, name: &String, property: UniquePtr<Value>);
    }

    #[namespace = "kuzu_rs"]
    unsafe extern "C++" {
        type PropertyList<'a>;

        fn size<'a>(&'a self) -> usize;
        fn get_name<'a>(&'a self, index: usize) -> String;
        fn get_value<'a>(&'a self, index: usize) -> &'a Value;
    }

    #[namespace = "kuzu_rs"]
    unsafe extern "C++" {
        #[namespace = "kuzu::common"]
        type NodeVal;

        #[rust_name = "node_value_get_properties"]
        fn value_get_properties(node_value: &NodeVal) -> UniquePtr<PropertyList>;
        fn node_value_get_node_id(value: &NodeVal) -> [u64; 2];
        #[rust_name = "node_value_get_label_name"]
        fn value_get_label_name(value: &NodeVal) -> String;
    }

    #[namespace = "kuzu_rs"]
    unsafe extern "C++" {
        #[namespace = "kuzu::common"]
        type RelVal;

        #[rust_name = "rel_value_get_properties"]
        fn value_get_properties(node_value: &RelVal) -> UniquePtr<PropertyList>;
        #[rust_name = "rel_value_get_label_name"]
        fn value_get_label_name(value: &RelVal) -> String;

        fn rel_value_get_src_id(value: &RelVal) -> [u64; 2];
        fn rel_value_get_dst_id(value: &RelVal) -> [u64; 2];
    }
}
