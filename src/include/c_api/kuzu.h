#pragma once
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifdef __cplusplus
#define KUZU_C_API extern "C"
#else
#define KUZU_C_API
#endif

KUZU_C_API typedef struct { void* _database; } kuzu_database;

KUZU_C_API typedef struct { void* _connection; } kuzu_connection;

KUZU_C_API typedef struct {
    void* _prepared_statement;
    void* _bound_values;
} kuzu_prepared_statement;

KUZU_C_API typedef struct { void* _query_result; } kuzu_query_result;

KUZU_C_API typedef struct { void* _flat_tuple; } kuzu_flat_tuple;

KUZU_C_API typedef struct { void* _data_type; } kuzu_data_type;

KUZU_C_API typedef struct {
    void* _value;
    bool _is_owned_by_cpp;
} kuzu_value;

KUZU_C_API typedef struct {
    void* _node_val;
    bool _is_owned_by_cpp;
} kuzu_node_val;

KUZU_C_API typedef struct {
    void* _rel_val;
    bool _is_owned_by_cpp;
} kuzu_rel_val;

KUZU_C_API typedef struct {
    uint64_t table_id;
    uint64_t offset;
} kuzu_internal_id_t;

KUZU_C_API typedef struct {
    // Days since 1970-01-01 00:00:00 UTC.
    int32_t days;
} kuzu_date_t;

KUZU_C_API typedef struct {
    // Microseconds since 1970-01-01 00:00:00 UTC.
    int64_t value;
} kuzu_timestamp_t;

KUZU_C_API typedef struct {
    int32_t months;
    int32_t days;
    int64_t micros;
} kuzu_interval_t;

KUZU_C_API typedef struct { void* _query_summary; } kuzu_query_summary;

KUZU_C_API typedef enum {
    ANY = 0,
    NODE = 10,
    REL = 11,
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
    STRING = 50,
    VAR_LIST = 52,
} kuzu_data_type_id;

// Database
KUZU_C_API kuzu_database* kuzu_database_init(const char* database_path, uint64_t buffer_pool_size);
KUZU_C_API void kuzu_database_destroy(kuzu_database* database);
KUZU_C_API void kuzu_database_set_logging_level(const char* logging_level);

// Connection
KUZU_C_API kuzu_connection* kuzu_connection_init(kuzu_database* database);
KUZU_C_API void kuzu_connection_destroy(kuzu_connection* connection);
KUZU_C_API void kuzu_connection_begin_read_only_transaction(kuzu_connection* connection);
KUZU_C_API void kuzu_connection_begin_write_transaction(kuzu_connection* connection);
KUZU_C_API void kuzu_connection_commit(kuzu_connection* connection);
KUZU_C_API void kuzu_connection_rollback(kuzu_connection* connection);
KUZU_C_API void kuzu_connection_set_max_num_thread_for_exec(
    kuzu_connection* connection, uint64_t num_threads);
KUZU_C_API uint64_t kuzu_connection_get_max_num_thread_for_exec(kuzu_connection* connection);
KUZU_C_API kuzu_query_result* kuzu_connection_query(kuzu_connection* connection, const char* query);
KUZU_C_API kuzu_prepared_statement* kuzu_connection_prepare(
    kuzu_connection* connection, const char* query);
KUZU_C_API kuzu_query_result* kuzu_connection_execute(
    kuzu_connection* connection, kuzu_prepared_statement* prepared_statement);
KUZU_C_API char* kuzu_connection_get_node_table_names(kuzu_connection* connection);
KUZU_C_API char* kuzu_connection_get_rel_table_names(kuzu_connection* connection);
KUZU_C_API char* kuzu_connection_get_node_property_names(
    kuzu_connection* connection, const char* table_name);
KUZU_C_API char* kuzu_connection_get_rel_property_names(
    kuzu_connection* connection, const char* table_name);
KUZU_C_API void kuzu_connection_interrupt(kuzu_connection* connection);
KUZU_C_API void kuzu_connection_set_query_timeout(
    kuzu_connection* connection, uint64_t timeout_in_ms);

// PreparedStatement
KUZU_C_API void kuzu_prepared_statement_destroy(kuzu_prepared_statement* prepared_statement);
KUZU_C_API bool kuzu_prepared_statement_allow_active_transaction(
    kuzu_prepared_statement* prepared_statement);
KUZU_C_API bool kuzu_prepared_statement_is_success(kuzu_prepared_statement* prepared_statement);
KUZU_C_API char* kuzu_prepared_statement_get_error_message(
    kuzu_prepared_statement* prepared_statement);
KUZU_C_API void kuzu_prepared_statement_bind_bool(
    kuzu_prepared_statement* prepared_statement, char* param_name, bool value);
KUZU_C_API void kuzu_prepared_statement_bind_int64(
    kuzu_prepared_statement* prepared_statement, char* param_name, int64_t value);
KUZU_C_API void kuzu_prepared_statement_bind_int32(
    kuzu_prepared_statement* prepared_statement, char* param_name, int32_t value);
KUZU_C_API void kuzu_prepared_statement_bind_int16(
    kuzu_prepared_statement* prepared_statement, char* param_name, int16_t value);
KUZU_C_API void kuzu_prepared_statement_bind_double(
    kuzu_prepared_statement* prepared_statement, char* param_name, double value);
KUZU_C_API void kuzu_prepared_statement_bind_float(
    kuzu_prepared_statement* prepared_statement, char* param_name, float value);
KUZU_C_API void kuzu_prepared_statement_bind_date(
    kuzu_prepared_statement* prepared_statement, char* param_name, kuzu_date_t value);
KUZU_C_API void kuzu_prepared_statement_bind_timestamp(
    kuzu_prepared_statement* prepared_statement, char* param_name, kuzu_timestamp_t value);
KUZU_C_API void kuzu_prepared_statement_bind_interval(
    kuzu_prepared_statement* prepared_statement, char* param_name, kuzu_interval_t value);
KUZU_C_API void kuzu_prepared_statement_bind_string(
    kuzu_prepared_statement* prepared_statement, char* param_name, char* value);
KUZU_C_API void kuzu_prepared_statement_bind_value(
    kuzu_prepared_statement* prepared_statement, char* param_name, kuzu_value* value);

// QueryResult
KUZU_C_API void kuzu_query_result_destroy(kuzu_query_result* query_result);
KUZU_C_API bool kuzu_query_result_is_success(kuzu_query_result* query_result);
KUZU_C_API char* kuzu_query_result_get_error_message(kuzu_query_result* query_result);
KUZU_C_API uint64_t kuzu_query_result_get_num_columns(kuzu_query_result* query_result);
KUZU_C_API char* kuzu_query_result_get_column_name(kuzu_query_result* query_result, uint64_t index);
KUZU_C_API kuzu_data_type* kuzu_query_result_get_column_data_type(
    kuzu_query_result* query_result, uint64_t index);
KUZU_C_API uint64_t kuzu_query_result_get_num_tuples(kuzu_query_result* query_result);
KUZU_C_API kuzu_query_summary* kuzu_query_result_get_query_summary(kuzu_query_result* query_result);
KUZU_C_API bool kuzu_query_result_has_next(kuzu_query_result* query_result);
KUZU_C_API kuzu_flat_tuple* kuzu_query_result_get_next(kuzu_query_result* query_result);
KUZU_C_API char* kuzu_query_result_to_string(kuzu_query_result* query_result);
KUZU_C_API void kuzu_query_result_write_to_csv(kuzu_query_result* query_result,
    const char* file_path, char delimiter, char escape_char, char new_line);
KUZU_C_API void kuzu_query_result_reset_iterator(kuzu_query_result* query_result);

// FlatTuple
KUZU_C_API void kuzu_flat_tuple_destroy(kuzu_flat_tuple* flat_tuple);
KUZU_C_API kuzu_value* kuzu_flat_tuple_get_value(kuzu_flat_tuple* flat_tuple, uint64_t index);
KUZU_C_API char* kuzu_flat_tuple_to_string(kuzu_flat_tuple* flat_tuple);

// DataType
KUZU_C_API kuzu_data_type* kuzu_data_type_create(
    kuzu_data_type_id id, kuzu_data_type* child_type, uint64_t fixed_num_elements_in_list);
KUZU_C_API kuzu_data_type* kuzu_data_type_clone(kuzu_data_type* data_type);
KUZU_C_API void kuzu_data_type_destroy(kuzu_data_type* data_type);
KUZU_C_API bool kuzu_data_type_equals(kuzu_data_type* data_type1, kuzu_data_type* data_type2);
KUZU_C_API kuzu_data_type_id kuzu_data_type_get_id(kuzu_data_type* data_type);
KUZU_C_API kuzu_data_type* kuzu_data_type_get_child_type(kuzu_data_type* data_type);
KUZU_C_API uint64_t kuzu_data_type_get_fixed_num_elements_in_list(kuzu_data_type* data_type);

// Value
KUZU_C_API kuzu_value* kuzu_value_create_null();
KUZU_C_API kuzu_value* kuzu_value_create_null_with_data_type(kuzu_data_type* data_type);
KUZU_C_API bool kuzu_value_is_null(kuzu_value* value);
KUZU_C_API void kuzu_value_set_null(kuzu_value* value, bool is_null);
KUZU_C_API kuzu_value* kuzu_value_create_default(kuzu_data_type* data_type);
KUZU_C_API kuzu_value* kuzu_value_create_bool(bool val_);
KUZU_C_API kuzu_value* kuzu_value_create_int16(int16_t val_);
KUZU_C_API kuzu_value* kuzu_value_create_int32(int32_t val_);
KUZU_C_API kuzu_value* kuzu_value_create_int64(int64_t val_);
KUZU_C_API kuzu_value* kuzu_value_create_float(float val_);
KUZU_C_API kuzu_value* kuzu_value_create_double(double val_);
KUZU_C_API kuzu_value* kuzu_value_create_internal_id(kuzu_internal_id_t val_);
KUZU_C_API kuzu_value* kuzu_value_create_node_val(kuzu_node_val* val_);
KUZU_C_API kuzu_value* kuzu_value_create_rel_val(kuzu_rel_val* val_);
KUZU_C_API kuzu_value* kuzu_value_create_date(kuzu_date_t val_);
KUZU_C_API kuzu_value* kuzu_value_create_timestamp(kuzu_timestamp_t val_);
KUZU_C_API kuzu_value* kuzu_value_create_interval(kuzu_interval_t val_);
KUZU_C_API kuzu_value* kuzu_value_create_string(char* val_);
KUZU_C_API kuzu_value* kuzu_value_clone(kuzu_value* value);
KUZU_C_API void kuzu_value_copy(kuzu_value* value, kuzu_value* other);
KUZU_C_API void kuzu_value_destroy(kuzu_value* value);
KUZU_C_API uint64_t kuzu_value_get_list_size(kuzu_value* value);
KUZU_C_API kuzu_value* kuzu_value_get_list_element(kuzu_value* value, uint64_t index);
KUZU_C_API kuzu_data_type* kuzu_value_get_data_type(kuzu_value* value);
KUZU_C_API bool kuzu_value_get_bool(kuzu_value* value);
KUZU_C_API int16_t kuzu_value_get_int16(kuzu_value* value);
KUZU_C_API int32_t kuzu_value_get_int32(kuzu_value* value);
KUZU_C_API int64_t kuzu_value_get_int64(kuzu_value* value);
KUZU_C_API float kuzu_value_get_float(kuzu_value* value);
KUZU_C_API double kuzu_value_get_double(kuzu_value* value);
KUZU_C_API kuzu_internal_id_t kuzu_value_get_internal_id(kuzu_value* value);
KUZU_C_API kuzu_node_val* kuzu_value_get_node_val(kuzu_value* value);
KUZU_C_API kuzu_rel_val* kuzu_value_get_rel_val(kuzu_value* value);
KUZU_C_API kuzu_date_t kuzu_value_get_date(kuzu_value* value);
KUZU_C_API kuzu_timestamp_t kuzu_value_get_timestamp(kuzu_value* value);
KUZU_C_API kuzu_interval_t kuzu_value_get_interval(kuzu_value* value);
KUZU_C_API char* kuzu_value_get_string(kuzu_value* value);
KUZU_C_API char* kuzu_value_to_string(kuzu_value* value);

KUZU_C_API kuzu_node_val* kuzu_node_val_create(kuzu_internal_id_t id, char* label);
KUZU_C_API kuzu_node_val* kuzu_node_val_clone(kuzu_node_val* node_val);
KUZU_C_API void kuzu_node_val_destroy(kuzu_node_val* node_val);
KUZU_C_API kuzu_value* kuzu_node_val_get_id_val(kuzu_node_val* node_val);
KUZU_C_API kuzu_value* kuzu_node_val_get_label_val(kuzu_node_val* node_val);
KUZU_C_API kuzu_internal_id_t kuzu_node_val_get_id(kuzu_node_val* node_val);
KUZU_C_API char* kuzu_node_val_get_label_name(kuzu_node_val* node_val);
KUZU_C_API uint64_t kuzu_node_val_get_property_size(kuzu_node_val* node_val);
KUZU_C_API char* kuzu_node_val_get_property_name_at(kuzu_node_val* node_val, uint64_t index);
KUZU_C_API kuzu_value* kuzu_node_val_get_property_value_at(kuzu_node_val* node_val, uint64_t index);
KUZU_C_API void kuzu_node_val_add_property(
    kuzu_node_val* node_val, const char* key, kuzu_value* value);
KUZU_C_API char* kuzu_node_val_to_string(kuzu_node_val* node_val);

KUZU_C_API kuzu_rel_val* kuzu_rel_val_create(
    kuzu_internal_id_t src_id, kuzu_internal_id_t dst_id, char* label);
KUZU_C_API kuzu_rel_val* kuzu_rel_val_clone(kuzu_rel_val* rel_val);
KUZU_C_API void kuzu_rel_val_destroy(kuzu_rel_val* rel_val);
KUZU_C_API kuzu_value* kuzu_rel_val_get_src_id_val(kuzu_rel_val* rel_val);
KUZU_C_API kuzu_value* kuzu_rel_val_get_dst_id_val(kuzu_rel_val* rel_val);
KUZU_C_API kuzu_internal_id_t kuzu_rel_val_get_src_id(kuzu_rel_val* rel_val);
KUZU_C_API kuzu_internal_id_t kuzu_rel_val_get_dst_id(kuzu_rel_val* rel_val);
KUZU_C_API char* kuzu_rel_val_get_label_name(kuzu_rel_val* rel_val);
KUZU_C_API uint64_t kuzu_rel_val_get_property_size(kuzu_rel_val* rel_val);
KUZU_C_API char* kuzu_rel_val_get_property_name_at(kuzu_rel_val* rel_val, uint64_t index);
KUZU_C_API kuzu_value* kuzu_rel_val_get_property_value_at(kuzu_rel_val* rel_val, uint64_t index);
KUZU_C_API void kuzu_rel_val_add_property(kuzu_rel_val* rel_val, char* key, kuzu_value* value);
KUZU_C_API char* kuzu_rel_val_to_string(kuzu_rel_val* rel_val);

// QuerySummary
KUZU_C_API void kuzu_query_summary_destroy(kuzu_query_summary* query_summary);
KUZU_C_API double kuzu_query_summary_get_compiling_time(kuzu_query_summary* query_summary);
KUZU_C_API double kuzu_query_summary_get_execution_time(kuzu_query_summary* query_summary);

// TODO: Bind utility functions for kuzu_date_t, kuzu_timestamp_t, and kuzu_interval_t
#undef KUZU_C_API
