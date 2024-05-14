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
 * KuzuNative is a wrapper class for the native library.
 * It is used to load the native library and call the native functions.
 * This class is not intended to be used by end users.
 */
public class KuzuNative {
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
            URL lib_res = KuzuNative.class.getResource(lib_res_name);
            if (lib_res == null) {
                throw new IOException(lib_res_name + " not found");
            }
            Files.copy(lib_res.openStream(), lib_file, StandardCopyOption.REPLACE_EXISTING);
            new File(lib_file.toString()).deleteOnExit();
            String lib_path = lib_file.toAbsolutePath().toString();
            System.load(lib_path);
            if(os_name.equals("linux")) {
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

    protected static native void kuzu_database_destroy(KuzuDatabase db);

    protected static native void kuzu_database_set_logging_level(String logging_level);

    // Connection
    protected static native long kuzu_connection_init(KuzuDatabase database);

    protected static native void kuzu_connection_destroy(KuzuConnection connection);

    protected static native void kuzu_connection_set_max_num_thread_for_exec(
            KuzuConnection connection, long num_threads);

    protected static native long kuzu_connection_get_max_num_thread_for_exec(KuzuConnection connection);

    protected static native KuzuQueryResult kuzu_connection_query(KuzuConnection connection, String query);

    protected static native KuzuPreparedStatement kuzu_connection_prepare(
            KuzuConnection connection, String query);

    protected static native KuzuQueryResult kuzu_connection_execute(
            KuzuConnection connection, KuzuPreparedStatement prepared_statement, Map<String, KuzuValue> param);

    protected static native void kuzu_connection_interrupt(KuzuConnection connection);

    protected static native void kuzu_connection_set_query_timeout(
            KuzuConnection connection, long timeout_in_ms);

    // PreparedStatement
    protected static native void kuzu_prepared_statement_destroy(KuzuPreparedStatement prepared_statement);

    protected static native boolean kuzu_prepared_statement_is_success(KuzuPreparedStatement prepared_statement);

    protected static native String kuzu_prepared_statement_get_error_message(
            KuzuPreparedStatement prepared_statement);

    // QueryResult
    protected static native void kuzu_query_result_destroy(KuzuQueryResult query_result);

    protected static native boolean kuzu_query_result_is_success(KuzuQueryResult query_result);

    protected static native String kuzu_query_result_get_error_message(KuzuQueryResult query_result);

    protected static native long kuzu_query_result_get_num_columns(KuzuQueryResult query_result);

    protected static native String kuzu_query_result_get_column_name(KuzuQueryResult query_result, long index);

    protected static native KuzuDataType kuzu_query_result_get_column_data_type(
            KuzuQueryResult query_result, long index);

    protected static native long kuzu_query_result_get_num_tuples(KuzuQueryResult query_result);

    protected static native KuzuQuerySummary kuzu_query_result_get_query_summary(KuzuQueryResult query_result);

    protected static native boolean kuzu_query_result_has_next(KuzuQueryResult query_result);

    protected static native KuzuFlatTuple kuzu_query_result_get_next(KuzuQueryResult query_result);

    protected static native String kuzu_query_result_to_string(KuzuQueryResult query_result);

    protected static native void kuzu_query_result_reset_iterator(KuzuQueryResult query_result);

    // FlatTuple
    protected static native void kuzu_flat_tuple_destroy(KuzuFlatTuple flat_tuple);

    protected static native KuzuValue kuzu_flat_tuple_get_value(KuzuFlatTuple flat_tuple, long index);

    protected static native String kuzu_flat_tuple_to_string(KuzuFlatTuple flat_tuple);

    // DataType
    protected static native long kuzu_data_type_create(
            KuzuDataTypeID id, KuzuDataType child_type, long num_elements_in_array);

    protected static native KuzuDataType kuzu_data_type_clone(KuzuDataType data_type);

    protected static native void kuzu_data_type_destroy(KuzuDataType data_type);

    protected static native boolean kuzu_data_type_equals(KuzuDataType data_type1, KuzuDataType data_type2);

    protected static native KuzuDataTypeID kuzu_data_type_get_id(KuzuDataType data_type);

    protected static native KuzuDataType kuzu_data_type_get_child_type(KuzuDataType data_type);

    protected static native long kuzu_data_type_get_num_elements_in_array(KuzuDataType data_type);

    // Value
    protected static native KuzuValue kuzu_value_create_null();

    protected static native KuzuValue kuzu_value_create_null_with_data_type(KuzuDataType data_type);

    protected static native boolean kuzu_value_is_null(KuzuValue value);

    protected static native void kuzu_value_set_null(KuzuValue value, boolean is_null);

    protected static native KuzuValue kuzu_value_create_default(KuzuDataType data_type);

    protected static native <T> long kuzu_value_create_value(T val);

    protected static native KuzuValue kuzu_value_clone(KuzuValue value);

    protected static native void kuzu_value_copy(KuzuValue value, KuzuValue other);

    protected static native void kuzu_value_destroy(KuzuValue value);

    protected static native long kuzu_value_get_list_size(KuzuValue value);

    protected static native KuzuValue kuzu_value_get_list_element(KuzuValue value, long index);

    protected static native KuzuDataType kuzu_value_get_data_type(KuzuValue value);

    protected static native <T> T kuzu_value_get_value(KuzuValue value);

    protected static native String kuzu_value_to_string(KuzuValue value);

    protected static native KuzuInternalID kuzu_node_val_get_id(KuzuValue node_val);

    protected static native String kuzu_node_val_get_label_name(KuzuValue node_val);

    protected static native long kuzu_node_val_get_property_size(KuzuValue node_val);

    protected static native String kuzu_node_val_get_property_name_at(KuzuValue node_val, long index);

    protected static native KuzuValue kuzu_node_val_get_property_value_at(KuzuValue node_val, long index);

    protected static native String kuzu_node_val_to_string(KuzuValue node_val);

    protected static native KuzuInternalID kuzu_rel_val_get_src_id(KuzuValue rel_val);

    protected static native KuzuInternalID kuzu_rel_val_get_dst_id(KuzuValue rel_val);

    protected static native String kuzu_rel_val_get_label_name(KuzuValue rel_val);

    protected static native long kuzu_rel_val_get_property_size(KuzuValue rel_val);

    protected static native String kuzu_rel_val_get_property_name_at(KuzuValue rel_val, long index);

    protected static native KuzuValue kuzu_rel_val_get_property_value_at(KuzuValue rel_val, long index);

    protected static native String kuzu_rel_val_to_string(KuzuValue rel_val);

    protected static native String kuzu_value_get_struct_field_name(KuzuValue struct_val, long index);

    protected static native long kuzu_value_get_struct_index(KuzuValue struct_val, String field_name);

    protected static native KuzuDataType kuzu_rdf_variant_get_data_type(KuzuValue rdf_variant);

    protected static native <T> T kuzu_rdf_variant_get_value(KuzuValue rdf_variant);

    protected static native String kuzu_get_version();

    protected static native long kuzu_get_storage_version();
}
