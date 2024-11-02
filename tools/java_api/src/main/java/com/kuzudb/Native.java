package com.kuzudb;

import java.util.Map;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Native is a wrapper class for the native library.
 * It is used to load the native library and call the native functions.
 * This class is not intended to be used by end users.
 */
public class Native {
    static {
        try {
            String os_name = "";
            String os_arch;
            String os_name_detect = System.getProperty("os.name").toLowerCase().trim();
            String os_arch_detect = System.getProperty("os.arch").toLowerCase().trim();
            switch (os_arch_detect) {
                case "x86_64":
                case "amd64":
                    os_arch = "amd64";
                    break;
                case "aarch64":
                case "arm64":
                    os_arch = "arm64";
                    break;
                case "i386":
                    os_arch = "i386";
                    break;
                default:
                    throw new IllegalStateException("Unsupported system architecture");
            }
            if (os_name_detect.startsWith("windows")) {
                os_name = "windows";
            } else if (os_name_detect.startsWith("mac")) {
                os_name = "osx";
            } else if (os_name_detect.startsWith("linux")) {
                os_name = "linux";
            }
            String lib_res_name = "/libkuzu_java_native.so" + "_" + os_name + "_" + os_arch;

            Path lib_file = Files.createTempFile("libkuzu_java_native", ".so");
            URL lib_res = Native.class.getResource(lib_res_name);
            if (lib_res == null) {
                throw new IOException(lib_res_name + " not found");
            }
            Files.copy(lib_res.openStream(), lib_file, StandardCopyOption.REPLACE_EXISTING);
            new File(lib_file.toString()).deleteOnExit();
            String lib_path = lib_file.toAbsolutePath().toString();
            System.load(lib_path);
            if (os_name.equals("linux")) {
                kuzu_native_reload_library(lib_path);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Hack: Reload the native library again in JNI bindings to work around the 
    // extension loading issue on Linux as System.load() does not set 
    // `RTLD_GLOBAL` flag and there is no way to set it in Java.
    protected static native void kuzu_native_reload_library(String lib_path);

    // Database
    protected static native long kuzu_database_init(String database_path, long buffer_pool_size, boolean enable_compression, boolean read_only, long max_db_size);

    protected static native void kuzu_database_destroy(Database db);

    protected static native void kuzu_database_set_logging_level(String logging_level);

    // Connection
    protected static native long kuzu_connection_init(Database database);

    protected static native void kuzu_connection_destroy(Connection connection);

    protected static native void kuzu_connection_set_max_num_thread_for_exec(
            Connection connection, long num_threads);

    protected static native long kuzu_connection_get_max_num_thread_for_exec(Connection connection);

    protected static native QueryResult kuzu_connection_query(Connection connection, String query);

    protected static native PreparedStatement kuzu_connection_prepare(
            Connection connection, String query);

    protected static native QueryResult kuzu_connection_execute(
            Connection connection, PreparedStatement prepared_statement, Map<String, Value> param);

    protected static native void kuzu_connection_interrupt(Connection connection);

    protected static native void kuzu_connection_set_query_timeout(
            Connection connection, long timeout_in_ms);

    // PreparedStatement
    protected static native void kuzu_prepared_statement_destroy(PreparedStatement prepared_statement);

    protected static native boolean kuzu_prepared_statement_is_success(PreparedStatement prepared_statement);

    protected static native String kuzu_prepared_statement_get_error_message(
            PreparedStatement prepared_statement);

    // QueryResult
    protected static native void kuzu_query_result_destroy(QueryResult query_result);

    protected static native boolean kuzu_query_result_is_success(QueryResult query_result);

    protected static native String kuzu_query_result_get_error_message(QueryResult query_result);

    protected static native long kuzu_query_result_get_num_columns(QueryResult query_result);

    protected static native String kuzu_query_result_get_column_name(QueryResult query_result, long index);

    protected static native DataType kuzu_query_result_get_column_data_type(
            QueryResult query_result, long index);

    protected static native long kuzu_query_result_get_num_tuples(QueryResult query_result);

    protected static native QuerySummary kuzu_query_result_get_query_summary(QueryResult query_result);

    protected static native boolean kuzu_query_result_has_next(QueryResult query_result);

    protected static native FlatTuple kuzu_query_result_get_next(QueryResult query_result);

    protected static native boolean kuzu_query_result_has_next_query_result(QueryResult query_result);

    protected static native QueryResult kuzu_query_result_get_next_query_result(QueryResult query_result);

    protected static native String kuzu_query_result_to_string(QueryResult query_result);

    protected static native void kuzu_query_result_reset_iterator(QueryResult query_result);

    // FlatTuple
    protected static native void kuzu_flat_tuple_destroy(FlatTuple flat_tuple);

    protected static native Value kuzu_flat_tuple_get_value(FlatTuple flat_tuple, long index);

    protected static native String kuzu_flat_tuple_to_string(FlatTuple flat_tuple);

    // DataType
    protected static native long kuzu_data_type_create(
            DataTypeID id, DataType child_type, long num_elements_in_array);

    protected static native DataType kuzu_data_type_clone(DataType data_type);

    protected static native void kuzu_data_type_destroy(DataType data_type);

    protected static native boolean kuzu_data_type_equals(DataType data_type1, DataType data_type2);

    protected static native DataTypeID kuzu_data_type_get_id(DataType data_type);

    protected static native DataType kuzu_data_type_get_child_type(DataType data_type);

    protected static native long kuzu_data_type_get_num_elements_in_array(DataType data_type);

    // Value
    protected static native Value kuzu_value_create_null();

    protected static native Value kuzu_value_create_null_with_data_type(DataType data_type);

    protected static native boolean kuzu_value_is_null(Value value);

    protected static native void kuzu_value_set_null(Value value, boolean is_null);

    protected static native Value kuzu_value_create_default(DataType data_type);

    protected static native <T> long kuzu_value_create_value(T val);

    protected static native Value kuzu_value_clone(Value value);

    protected static native void kuzu_value_copy(Value value, Value other);

    protected static native void kuzu_value_destroy(Value value);

    protected static native long kuzu_value_get_list_size(Value value);

    protected static native Value kuzu_value_get_list_element(Value value, long index);

    protected static native DataType kuzu_value_get_data_type(Value value);

    protected static native <T> T kuzu_value_get_value(Value value);

    protected static native String kuzu_value_to_string(Value value);

    protected static native InternalID kuzu_node_val_get_id(Value node_val);

    protected static native String kuzu_node_val_get_label_name(Value node_val);

    protected static native long kuzu_node_val_get_property_size(Value node_val);

    protected static native String kuzu_node_val_get_property_name_at(Value node_val, long index);

    protected static native Value kuzu_node_val_get_property_value_at(Value node_val, long index);

    protected static native String kuzu_node_val_to_string(Value node_val);

    protected static native InternalID kuzu_rel_val_get_src_id(Value rel_val);

    protected static native InternalID kuzu_rel_val_get_dst_id(Value rel_val);

    protected static native String kuzu_rel_val_get_label_name(Value rel_val);

    protected static native long kuzu_rel_val_get_property_size(Value rel_val);

    protected static native String kuzu_rel_val_get_property_name_at(Value rel_val, long index);

    protected static native Value kuzu_rel_val_get_property_value_at(Value rel_val, long index);

    protected static native String kuzu_rel_val_to_string(Value rel_val);

    protected static native String kuzu_value_get_struct_field_name(Value struct_val, long index);

    protected static native long kuzu_value_get_struct_index(Value struct_val, String field_name);

    protected static native String kuzu_get_version();

    protected static native long kuzu_get_storage_version();
}
