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

/**
 * @brief kuzu_database manages all database components.
 */
KUZU_C_API typedef struct { void* _database; } kuzu_database;

/**
 * @brief kuzu_connection is used to interact with a Database instance. Each connection is
 * thread-safe. Multiple connections can connect to the same Database instance in a multi-threaded
 * environment.
 */
KUZU_C_API typedef struct { void* _connection; } kuzu_connection;

/**
 * @brief kuzu_prepared_statement is a parameterized query which can avoid planning the same query
 * for repeated execution.
 */
KUZU_C_API typedef struct {
    void* _prepared_statement;
    void* _bound_values;
} kuzu_prepared_statement;

/**
 * @brief kuzu_query_result stores the result of a query.
 */
KUZU_C_API typedef struct { void* _query_result; } kuzu_query_result;

/**
 * @brief kuzu_flat_tuple stores a vector of values.
 */
KUZU_C_API typedef struct { void* _flat_tuple; } kuzu_flat_tuple;

/**
 * @brief kuzu_logical_type is the kuzu internal representation of data types.
 */
KUZU_C_API typedef struct { void* _data_type; } kuzu_logical_type;

/**
 * @brief kuzu_value is used to represent a value with any kuzu internal dataType.
 */
KUZU_C_API typedef struct {
    void* _value;
    bool _is_owned_by_cpp;
} kuzu_value;

/**
 * @brief kuzu internal node type which stores the nodeID, label and properties of a node in graph.
 */
KUZU_C_API typedef struct {
    void* _node_val;
    bool _is_owned_by_cpp;
} kuzu_node_val;

/**
 * @brief kuzu internal rel type which stores the relID, src/dst nodes and properties of a rel in
 * graph.
 */
KUZU_C_API typedef struct {
    void* _rel_val;
    bool _is_owned_by_cpp;
} kuzu_rel_val;

/**
 * @brief kuzu internal internal_id type which stores the table_id and offset of a node/rel.
 */
KUZU_C_API typedef struct {
    uint64_t table_id;
    uint64_t offset;
} kuzu_internal_id_t;

/**
 * @brief kuzu internal date type which stores the number of days since 1970-01-01 00:00:00 UTC.
 */
KUZU_C_API typedef struct {
    // Days since 1970-01-01 00:00:00 UTC.
    int32_t days;
} kuzu_date_t;

/**
 * @brief kuzu internal timestamp type which stores the number of microseconds since 1970-01-01
 * 00:00:00 UTC.
 */
KUZU_C_API typedef struct {
    // Microseconds since 1970-01-01 00:00:00 UTC.
    int64_t value;
} kuzu_timestamp_t;

/**
 * @brief kuzu internal interval type which stores the months, days and microseconds.
 */
KUZU_C_API typedef struct {
    int32_t months;
    int32_t days;
    int64_t micros;
} kuzu_interval_t;

/**
 * @brief kuzu_query_summary stores the execution time, plan, compiling time and query options of a
 * query.
 */
KUZU_C_API typedef struct { void* _query_summary; } kuzu_query_summary;

/**
 * @brief enum class for kuzu internal dataTypes.
 */
KUZU_C_API typedef enum {
    KUZU_ANY = 0,
    KUZU_NODE = 10,
    KUZU_REL = 11,
    KUZU_BOOL = 22,
    KUZU_INT64 = 23,
    KUZU_INT32 = 24,
    KUZU_INT16 = 25,
    KUZU_DOUBLE = 26,
    KUZU_FLOAT = 27,
    KUZU_DATE = 28,
    KUZU_TIMESTAMP = 29,
    KUZU_INTERVAL = 30,
    KUZU_FIXED_LIST = 31,
    KUZU_INTERNAL_ID = 40,
    KUZU_STRING = 50,
    KUZU_VAR_LIST = 52,
    KUZU_STRUCT = 53,
} kuzu_data_type_id;

// Database
/**
 * @brief Allocates memory and creates a kuzu database instance at database_path with
 * bufferPoolSize=buffer_pool_size. Caller is responsible for calling kuzu_database_destroy() to
 * release the allocated memory.
 * @param database_path The path to the database.
 * @param buffer_pool_size The size of the buffer pool in bytes.
 * @return The database instance.
 */
KUZU_C_API kuzu_database* kuzu_database_init(const char* database_path, uint64_t buffer_pool_size);
/**
 * @brief Destroys the kuzu database instance and frees the allocated memory.
 * @param database The database instance to destroy.
 */
KUZU_C_API void kuzu_database_destroy(kuzu_database* database);
/**
 * @brief Sets the logging level of the database.
 * @param logging_level The logging level to set. Supported logging levels are: "info", "debug",
 * "err".
 */
KUZU_C_API void kuzu_database_set_logging_level(const char* logging_level);

// Connection
/**
 * @brief Allocates memory and creates a connection to the database. Caller is responsible for
 * calling kuzu_connection_destroy() to release the allocated memory.
 * @param database The database instance to connect to.
 * @return The connection instance.
 */
KUZU_C_API kuzu_connection* kuzu_connection_init(kuzu_database* database);
/**
 * @brief Destroys the connection instance and frees the allocated memory.
 * @param connection The connection instance to destroy.
 */
KUZU_C_API void kuzu_connection_destroy(kuzu_connection* connection);
/**
 * @brief Begins a read-only transaction in the given connection.
 * @param connection The connection instance to begin read-only transaction.
 */
KUZU_C_API void kuzu_connection_begin_read_only_transaction(kuzu_connection* connection);
/**
 * @brief Begins a write transaction in the given connection.
 * @param connection The connection instance to begin write transaction.
 */
KUZU_C_API void kuzu_connection_begin_write_transaction(kuzu_connection* connection);
/**
 * @brief Commits the current transaction.
 * @param connection The connection instance to commit transaction.
 */
KUZU_C_API void kuzu_connection_commit(kuzu_connection* connection);
/**
 * @brief Rollbacks the current transaction.
 * @param connection The connection instance to rollback transaction.
 */
KUZU_C_API void kuzu_connection_rollback(kuzu_connection* connection);
/**
 * @brief Sets the maximum number of threads to use for executing queries.
 * @param connection The connection instance to set max number of threads for execution.
 * @param num_threads The maximum number of threads to use for executing queries.
 */
KUZU_C_API void kuzu_connection_set_max_num_thread_for_exec(
    kuzu_connection* connection, uint64_t num_threads);

/**
 * @brief Returns the maximum number of threads of the connection to use for executing queries.
 * @param connection The connection instance to return max number of threads for execution.
 */
KUZU_C_API uint64_t kuzu_connection_get_max_num_thread_for_exec(kuzu_connection* connection);
/**
 * @brief Executes the given query and returns the result.
 * @param connection The connection instance to execute the query.
 * @param query The query to execute.
 * @return the result of the query.
 */
KUZU_C_API kuzu_query_result* kuzu_connection_query(kuzu_connection* connection, const char* query);
/**
 * @brief Prepares the given query and returns the prepared statement.
 * @param connection The connection instance to prepare the query.
 * @param query The query to prepare.
 */
KUZU_C_API kuzu_prepared_statement* kuzu_connection_prepare(
    kuzu_connection* connection, const char* query);
/**
 * @brief Executes the prepared_statement using connection.
 * @param connection The connection instance to execute the prepared_statement.
 * @param prepared_statement The prepared statement to execute.
 */
KUZU_C_API kuzu_query_result* kuzu_connection_execute(
    kuzu_connection* connection, kuzu_prepared_statement* prepared_statement);
/**
 * @brief Returns all node table names of the database.
 * @param connection The connection instance to return all node table names.
 */
KUZU_C_API char* kuzu_connection_get_node_table_names(kuzu_connection* connection);
/**
 * @brief Returns all rel table names of the database.
 * @param connection The connection instance to return all rel table names.
 */
KUZU_C_API char* kuzu_connection_get_rel_table_names(kuzu_connection* connection);
/**
 * @brief Returns all property names of the given node table.
 * @param connection The connection instance to return all property names.
 * @param table_name The table name to return all property names.
 */
KUZU_C_API char* kuzu_connection_get_node_property_names(
    kuzu_connection* connection, const char* table_name);
/**
 * @brief Returns all property names of the given rel table.
 * @param connection The connection instance to return all property names.
 * @param table_name The table name to return all property names.
 */
KUZU_C_API char* kuzu_connection_get_rel_property_names(
    kuzu_connection* connection, const char* table_name);
/**
 * @brief Interrupts the current query execution in the connection.
 * @param connection The connection instance to interrupt.
 */
KUZU_C_API void kuzu_connection_interrupt(kuzu_connection* connection);
/**
 * @brief Sets query timeout value in milliseconds for the connection.
 * @param connection The connection instance to set query timeout value.
 * @param timeout_in_ms The timeout value in milliseconds.
 */
KUZU_C_API void kuzu_connection_set_query_timeout(
    kuzu_connection* connection, uint64_t timeout_in_ms);

// PreparedStatement
/**
 * @brief Destroys the prepared statement instance and frees the allocated memory.
 * @param prepared_statement The prepared statement instance to destroy.
 */
KUZU_C_API void kuzu_prepared_statement_destroy(kuzu_prepared_statement* prepared_statement);
/**
 * @brief DDL and COPY statements are automatically wrapped in a transaction and committed.
 * As such, they cannot be part of an active transaction.
 * @return the prepared statement is allowed to be part of an active transaction.
 */
KUZU_C_API bool kuzu_prepared_statement_allow_active_transaction(
    kuzu_prepared_statement* prepared_statement);
/**
 * @return the query is prepared successfully or not.
 */
KUZU_C_API bool kuzu_prepared_statement_is_success(kuzu_prepared_statement* prepared_statement);
/**
 * @return the error message if the statement is not prepared successfully.
 */
KUZU_C_API char* kuzu_prepared_statement_get_error_message(
    kuzu_prepared_statement* prepared_statement);
/**
 * @brief Binds the given boolean value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The boolean value to bind.
 */
KUZU_C_API void kuzu_prepared_statement_bind_bool(
    kuzu_prepared_statement* prepared_statement, const char* param_name, bool value);
/**
 * @brief Binds the given int64_t value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The int64_t value to bind.
 */
KUZU_C_API void kuzu_prepared_statement_bind_int64(
    kuzu_prepared_statement* prepared_statement, const char* param_name, int64_t value);
/**
 * @brief Binds the given int32_t value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The int32_t value to bind.
 */
KUZU_C_API void kuzu_prepared_statement_bind_int32(
    kuzu_prepared_statement* prepared_statement, const char* param_name, int32_t value);
/**
 * @brief Binds the given int16_t value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The int16_t value to bind.
 */
KUZU_C_API void kuzu_prepared_statement_bind_int16(
    kuzu_prepared_statement* prepared_statement, const char* param_name, int16_t value);
/**
 * @brief Binds the given double value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The double value to bind.
 */
KUZU_C_API void kuzu_prepared_statement_bind_double(
    kuzu_prepared_statement* prepared_statement, const char* param_name, double value);
/**
 * @brief Binds the given float value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The float value to bind.
 */
KUZU_C_API void kuzu_prepared_statement_bind_float(
    kuzu_prepared_statement* prepared_statement, const char* param_name, float value);
/**
 * @brief Binds the given date value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The date value to bind.
 */
KUZU_C_API void kuzu_prepared_statement_bind_date(
    kuzu_prepared_statement* prepared_statement, const char* param_name, kuzu_date_t value);
/**
 * @brief Binds the given timestamp value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The timestamp value to bind.
 */
KUZU_C_API void kuzu_prepared_statement_bind_timestamp(
    kuzu_prepared_statement* prepared_statement, const char* param_name, kuzu_timestamp_t value);
/**
 * @brief Binds the given interval value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The interval value to bind.
 */
KUZU_C_API void kuzu_prepared_statement_bind_interval(
    kuzu_prepared_statement* prepared_statement, const char* param_name, kuzu_interval_t value);
/**
 * @brief Binds the given string value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The string value to bind.
 */
KUZU_C_API void kuzu_prepared_statement_bind_string(
    kuzu_prepared_statement* prepared_statement, const char* param_name, const char* value);
/**
 * @brief Binds the given kuzu value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The kuzu value to bind.
 */
KUZU_C_API void kuzu_prepared_statement_bind_value(
    kuzu_prepared_statement* prepared_statement, const char* param_name, kuzu_value* value);

// QueryResult
/**
 * @brief Destroys the given query result instance.
 * @param query_result The query result instance to destroy.
 */
KUZU_C_API void kuzu_query_result_destroy(kuzu_query_result* query_result);
/**
 * @brief Returns true if the query is executed successful, false otherwise.
 * @param query_result The query result instance to check.
 */
KUZU_C_API bool kuzu_query_result_is_success(kuzu_query_result* query_result);
/**
 * @brief Returns the error message if the query is failed.
 * @param query_result The query result instance to check and return error message.
 */
KUZU_C_API char* kuzu_query_result_get_error_message(kuzu_query_result* query_result);
/**
 * @brief Returns the number of columns in the query result.
 * @param query_result The query result instance to return.
 */
KUZU_C_API uint64_t kuzu_query_result_get_num_columns(kuzu_query_result* query_result);
/**
 * @brief Returns the column name at the given index.
 * @param query_result The query result instance to return.
 * @param index The index of the column to return name.
 */
KUZU_C_API char* kuzu_query_result_get_column_name(kuzu_query_result* query_result, uint64_t index);
/**
 * @brief Returns the data type of the column at the given index.
 * @param query_result The query result instance to return.
 * @param index The index of the column to return data type.
 */
KUZU_C_API kuzu_logical_type* kuzu_query_result_get_column_data_type(
    kuzu_query_result* query_result, uint64_t index);
/**
 * @brief Returns the number of tuples in the query result.
 * @param query_result The query result instance to return.
 */
KUZU_C_API uint64_t kuzu_query_result_get_num_tuples(kuzu_query_result* query_result);
/**
 * @brief Returns the query summary of the query result.
 * @param query_result The query result instance to return.
 */
KUZU_C_API kuzu_query_summary* kuzu_query_result_get_query_summary(kuzu_query_result* query_result);
/**
 * @brief Returns true if we have not consumed all tuples in the query result, false otherwise.
 * @param query_result The query result instance to check.
 */
KUZU_C_API bool kuzu_query_result_has_next(kuzu_query_result* query_result);
/**
 * @brief Returns the next tuple in the query result. Throws an exception if there is no more tuple.
 * @param query_result The query result instance to return.
 */
KUZU_C_API kuzu_flat_tuple* kuzu_query_result_get_next(kuzu_query_result* query_result);
/**
 * @brief Returns the query result as a string.
 * @param query_result The query result instance to return.
 */
KUZU_C_API char* kuzu_query_result_to_string(kuzu_query_result* query_result);
/**
 * @brief Writes the query result to the given file path as CSV.
 * @param query_result The query result instance to write.
 * @param file_path The file path to write the query result.
 * @param delimiter The delimiter character to use when writing csv file.
 * @param escape_char The escape character to use when writing csv file.
 * @param new_line The new line character to use when writing csv file.
 */
KUZU_C_API void kuzu_query_result_write_to_csv(kuzu_query_result* query_result,
    const char* file_path, char delimiter, char escape_char, char new_line);
/**
 * @brief Resets the iterator of the query result to the beginning of the query result.
 * @param query_result The query result instance to reset iterator.
 */
KUZU_C_API void kuzu_query_result_reset_iterator(kuzu_query_result* query_result);

// FlatTuple
/**
 * @brief Destroys the given flat tuple instance.
 * @param flat_tuple The flat tuple instance to destroy.
 */
KUZU_C_API void kuzu_flat_tuple_destroy(kuzu_flat_tuple* flat_tuple);
/**
 * @brief Returns the value at index of the flat tuple.
 * @param flat_tuple The flat tuple instance to return.
 * @param index The index of the value to return.
 */
KUZU_C_API kuzu_value* kuzu_flat_tuple_get_value(kuzu_flat_tuple* flat_tuple, uint64_t index);
/**
 * @brief Converts the flat tuple to a string.
 * @param flat_tuple The flat tuple instance to convert.
 */
KUZU_C_API char* kuzu_flat_tuple_to_string(kuzu_flat_tuple* flat_tuple);

// DataType
// TODO(Chang): Refactor the datatype constructor to follow the cpp way of creating dataTypes.
/**
 * @brief Creates a data type instance with the given id, childType and fixed_num_elements_in_list.
 * Caller is responsible for destroying the returned data type instance.
 * @param id The enum type id of the datatype to create.
 * @param child_type The child type of the datatype to create(only used for nested dataTypes).
 * @param fixed_num_elements_in_list The fixed number of elements in the list(only used for
 * FIXED_LIST).
 */
KUZU_C_API kuzu_logical_type* kuzu_data_type_create(
    kuzu_data_type_id id, kuzu_logical_type* child_type, uint64_t fixed_num_elements_in_list);
/**
 * @brief Creates a new data type instance by cloning the given data type instance.
 * @param data_type The data type instance to clone.
 */
KUZU_C_API kuzu_logical_type* kuzu_data_type_clone(kuzu_logical_type* data_type);
/**
 * @brief Destroys the given data type instance.
 * @param data_type The data type instance to destroy.
 */
KUZU_C_API void kuzu_data_type_destroy(kuzu_logical_type* data_type);
/**
 * @brief Returns true if the given data type is equal to the other data type, false otherwise.
 * @param data_type1 The first data type instance to compare.
 * @param data_type2 The second data type instance to compare.
 */
KUZU_C_API bool kuzu_data_type_equals(kuzu_logical_type* data_type1, kuzu_logical_type* data_type2);
/**
 * @brief Returns the enum type id of the given data type.
 * @param data_type The data type instance to return.
 */
KUZU_C_API kuzu_data_type_id kuzu_data_type_get_id(kuzu_logical_type* data_type);
/**
 * @brief Returns the number of elements per list for fixedSizeList.
 * @param data_type The data type instance to return.
 */
KUZU_C_API uint64_t kuzu_data_type_get_fixed_num_elements_in_list(kuzu_logical_type* data_type);

// Value
/**
 * @brief Creates a NULL value of ANY type. Caller is responsible for destroying the returned value.
 */
KUZU_C_API kuzu_value* kuzu_value_create_null();
/**
 * @brief Creates a value of the given data type. Caller is responsible for destroying the
 * returned value.
 * @param data_type The data type of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_null_with_data_type(kuzu_logical_type* data_type);
/**
 * @brief Returns true if the given value is NULL, false otherwise.
 * @param value The value instance to check.
 */
KUZU_C_API bool kuzu_value_is_null(kuzu_value* value);
/**
 * @brief Sets the given value to NULL or not.
 * @param value The value instance to set.
 * @param is_null True if sets the value to NULL, false otherwise.
 */
KUZU_C_API void kuzu_value_set_null(kuzu_value* value, bool is_null);
/**
 * @brief Creates a value of the given data type with default non-NULL value. Caller is responsible
 * for destroying the returned value.
 * @param data_type The data type of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_default(kuzu_logical_type* data_type);
/**
 * @brief Creates a value with boolean type and the given bool value. Caller is responsible for
 * destroying the returned value.
 * @param val_ The bool value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_bool(bool val_);
/**
 * @brief Creates a value with int16 type and the given int16 value. Caller is responsible for
 * destroying the returned value.
 * @param val_ The int16 value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_int16(int16_t val_);
/**
 * @brief Creates a value with int32 type and the given int32 value. Caller is responsible for
 * destroying the returned value.
 * @param val_ The int32 value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_int32(int32_t val_);
/**
 * @brief Creates a value with int64 type and the given int64 value. Caller is responsible for
 * destroying the returned value.
 * @param val_ The int64 value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_int64(int64_t val_);
/**
 * @brief Creates a value with float type and the given float value. Caller is responsible for
 * destroying the returned value.
 * @param val_ The float value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_float(float val_);
/**
 * @brief Creates a value with double type and the given double value. Caller is responsible for
 * destroying the returned value.
 * @param val_ The double value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_double(double val_);
/**
 * @brief Creates a value with internal_id type and the given internal_id value. Caller is
 * responsible for destroying the returned value.
 * @param val_ The internal_id value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_internal_id(kuzu_internal_id_t val_);
/**
 * @brief Creates a value with nodeVal type and the given nodeVal value. Caller is responsible for
 * destroying the returned value.
 * @param val_ The nodeVal value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_node_val(kuzu_node_val* val_);
/**
 * @brief Creates a value with relVal type and the given relVal value. Caller is responsible for
 * destroying the returned value.
 * @param val_ The relVal value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_rel_val(kuzu_rel_val* val_);
/**
 * @brief Creates a value with date type and the given date value. Caller is responsible for
 * destroying the returned value.
 * @param val_ The date value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_date(kuzu_date_t val_);
/**
 * @brief Creates a value with timestamp type and the given timestamp value. Caller is responsible
 * for destroying the returned value.
 * @param val_ The timestamp value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_timestamp(kuzu_timestamp_t val_);
/**
 * @brief Creates a value with interval type and the given interval value. Caller is responsible
 * for destroying the returned value.
 * @param val_ The interval value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_interval(kuzu_interval_t val_);
/**
 * @brief Creates a value with string type and the given string value. Caller is responsible for
 * destroying the returned value.
 * @param val_ The string value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_string(const char* val_);
/**
 * @brief Creates a new value based on the given value. Caller is responsible for destroying the
 * returned value.
 * @param value The value to create from.
 */
KUZU_C_API kuzu_value* kuzu_value_clone(kuzu_value* value);
/**
 * @brief Copies the other value to the value.
 * @param value The value to copy to.
 * @param other The value to copy from.
 */
KUZU_C_API void kuzu_value_copy(kuzu_value* value, kuzu_value* other);
/**
 * @brief Destroys the value.
 * @param value The value to destroy.
 */
KUZU_C_API void kuzu_value_destroy(kuzu_value* value);
/**
 * @brief Returns the number of elements per list of the given value. The value must be of type
 * FIXED_LIST.
 * @param value The FIXED_LIST value to get list size.
 */
KUZU_C_API uint64_t kuzu_value_get_list_size(kuzu_value* value);
/**
 * @brief Returns the element at index of the given value. The value must be of type VAR_LIST.
 * @param value The VAR_LIST value to return.
 * @param index The index of the element to return.
 */
KUZU_C_API kuzu_value* kuzu_value_get_list_element(kuzu_value* value, uint64_t index);
/**
 * @brief Returns the number of fields of the given struct value. The value must be of type STRUCT.
 * @param value The STRUCT value to get number of fields.
 */
KUZU_C_API uint64_t kuzu_value_get_struct_num_fields(kuzu_value* value);
/**
 * @brief Returns the field name at index of the given struct value. The value must be of type
 * STRUCT.
 * @param value The STRUCT value to get field name.
 * @param index The index of the field name to return.
 */
KUZU_C_API char* kuzu_value_get_struct_field_name(kuzu_value* value, uint64_t index);
/**
 * @brief Returns the field value at index of the given struct value. The value must be of type
 * STRUCT.
 * @param value The STRUCT value to get field value.
 * @param index The index of the field value to return.
 */
KUZU_C_API kuzu_value* kuzu_value_get_struct_field_value(kuzu_value* value, uint64_t index);
/**
 * @brief Returns internal type of the given value.
 * @param value The value to return.
 */
KUZU_C_API kuzu_logical_type* kuzu_value_get_data_type(kuzu_value* value);
/**
 * @brief Returns the boolean value of the given value. The value must be of type BOOL.
 * @param value The value to return.
 */
KUZU_C_API bool kuzu_value_get_bool(kuzu_value* value);
/**
 * @brief Returns the int16 value of the given value. The value must be of type INT16.
 * @param value The value to return.
 */
KUZU_C_API int16_t kuzu_value_get_int16(kuzu_value* value);
/**
 * @brief Returns the int32 value of the given value. The value must be of type INT32.
 * @param value The value to return.
 */
KUZU_C_API int32_t kuzu_value_get_int32(kuzu_value* value);
/**
 * @brief Returns the int64 value of the given value. The value must be of type INT64.
 * @param value The value to return.
 */
KUZU_C_API int64_t kuzu_value_get_int64(kuzu_value* value);
/**
 * @brief Returns the float value of the given value. The value must be of type FLOAT.
 * @param value The value to return.
 */
KUZU_C_API float kuzu_value_get_float(kuzu_value* value);
/**
 * @brief Returns the double value of the given value. The value must be of type DOUBLE.
 * @param value The value to return.
 */
KUZU_C_API double kuzu_value_get_double(kuzu_value* value);
/**
 * @brief Returns the internal id value of the given value. The value must be of type INTERNAL_ID.
 * @param value The value to return.
 */
KUZU_C_API kuzu_internal_id_t kuzu_value_get_internal_id(kuzu_value* value);
/**
 * @brief Returns the node value of the given value. The value must be of type NODE.
 * @param value The value to return.
 */
KUZU_C_API kuzu_node_val* kuzu_value_get_node_val(kuzu_value* value);
/**
 * @brief Returns the rel value of the given value. The value must be of type REL.
 * @param value The value to return.
 */
KUZU_C_API kuzu_rel_val* kuzu_value_get_rel_val(kuzu_value* value);
/**
 * @brief Returns the date value of the given value. The value must be of type DATE.
 * @param value The value to return.
 */
KUZU_C_API kuzu_date_t kuzu_value_get_date(kuzu_value* value);
/**
 * @brief Returns the timestamp value of the given value. The value must be of type TIMESTAMP.
 * @param value The value to return.
 */
KUZU_C_API kuzu_timestamp_t kuzu_value_get_timestamp(kuzu_value* value);
/**
 * @brief Returns the interval value of the given value. The value must be of type INTERVAL.
 * @param value The value to return.
 */
KUZU_C_API kuzu_interval_t kuzu_value_get_interval(kuzu_value* value);
/**
 * @brief Returns the string value of the given value. The value must be of type STRING.
 * @param value The value to return.
 */
KUZU_C_API char* kuzu_value_get_string(kuzu_value* value);
/**
 * @brief Converts the given value to string.
 * @param value The value to convert.
 */
KUZU_C_API char* kuzu_value_to_string(kuzu_value* value);

/**
 * @brief Creates a new node value.
 * @param id The internal id of the node.
 * @param label The label of the node.
 */
KUZU_C_API kuzu_node_val* kuzu_node_val_create(kuzu_internal_id_t id, const char* label);
/**
 * @brief Creates a new node value from the given node value.
 * @param node_val The node value to clone.
 */
KUZU_C_API kuzu_node_val* kuzu_node_val_clone(kuzu_node_val* node_val);
/**
 * @brief Destroys the given node value.
 * @param node_val The node value to destroy.
 */
KUZU_C_API void kuzu_node_val_destroy(kuzu_node_val* node_val);
/**
 * @brief Returns the internal id value of the given node value as a kuzu value.
 * @param node_val The node value to return.
 */
KUZU_C_API kuzu_value* kuzu_node_val_get_id_val(kuzu_node_val* node_val);
/**
 * @brief Returns the label value of the given node value as a label value.
 * @param node_val The node value to return.
 */
KUZU_C_API kuzu_value* kuzu_node_val_get_label_val(kuzu_node_val* node_val);
/**
 * @brief Returns the internal id value of the given node value as internal_id.
 * @param node_val The node value to return.
 */
KUZU_C_API kuzu_internal_id_t kuzu_node_val_get_id(kuzu_node_val* node_val);
/**
 * @brief Returns the label value of the given node value as string.
 * @param node_val The node value to return.
 */
KUZU_C_API char* kuzu_node_val_get_label_name(kuzu_node_val* node_val);
/**
 * @brief Returns the number of properties of the given node value.
 * @param node_val The node value to return.
 */
KUZU_C_API uint64_t kuzu_node_val_get_property_size(kuzu_node_val* node_val);
/**
 * @brief Returns the property name of the given node value at the given index.
 * @param node_val The node value to return.
 * @param index The index of the property.
 */
KUZU_C_API char* kuzu_node_val_get_property_name_at(kuzu_node_val* node_val, uint64_t index);
/**
 * @brief Returns the property value of the given node value at the given index.
 * @param node_val The node value to return.
 * @param index The index of the property.
 */
KUZU_C_API kuzu_value* kuzu_node_val_get_property_value_at(kuzu_node_val* node_val, uint64_t index);
/**
 * @brief Adds the property with name to the given node value.
 * @param node_val The node value to add to.
 * @param name The name of the property.
 * @param property The property(in value format) to add.
 */
KUZU_C_API void kuzu_node_val_add_property(
    kuzu_node_val* node_val, const char* name, kuzu_value* property);
/**
 * @brief Converts the given node value to string.
 * @param node_val The node value to convert.
 */
KUZU_C_API char* kuzu_node_val_to_string(kuzu_node_val* node_val);

/**
 * @brief Creates a new rel value. Caller is responsible for destroying the rel value.
 * @param src_id The internal id of the source node.
 * @param dst_id The internal id of the destination node.
 * @param label The label of the rel.
 */
KUZU_C_API kuzu_rel_val* kuzu_rel_val_create(
    kuzu_internal_id_t src_id, kuzu_internal_id_t dst_id, const char* label);
/**
 * @brief Creates a new rel value from the given rel value.
 * @param rel_val The rel value to clone.
 */
KUZU_C_API kuzu_rel_val* kuzu_rel_val_clone(kuzu_rel_val* rel_val);
/**
 * @brief Destroys the given rel value.
 * @param rel_val The rel value to destroy.
 */
KUZU_C_API void kuzu_rel_val_destroy(kuzu_rel_val* rel_val);
/**
 * @brief Returns the internal id value of the source node of the given rel value as a kuzu value.
 * @param rel_val The rel value to return.
 */
KUZU_C_API kuzu_value* kuzu_rel_val_get_src_id_val(kuzu_rel_val* rel_val);
/**
 * @brief Returns the internal id value of the destination node of the given rel value as a kuzu
 * value.
 * @param rel_val The rel value to return.
 */
KUZU_C_API kuzu_value* kuzu_rel_val_get_dst_id_val(kuzu_rel_val* rel_val);
/**
 * @brief Returns the internal id value of the source node of the given rel value.
 * @param rel_val The rel value to return.
 */
KUZU_C_API kuzu_internal_id_t kuzu_rel_val_get_src_id(kuzu_rel_val* rel_val);
/**
 * @brief Returns the internal id value of the destination node of the given rel value.
 * @param rel_val The rel value to return.
 */
KUZU_C_API kuzu_internal_id_t kuzu_rel_val_get_dst_id(kuzu_rel_val* rel_val);
/**
 * @brief Returns the label of the given rel value.
 * @param rel_val The rel value to return.
 */
KUZU_C_API char* kuzu_rel_val_get_label_name(kuzu_rel_val* rel_val);
/**
 * @brief Returns the number of properties of the given rel value.
 * @param rel_val The rel value to return.
 */
KUZU_C_API uint64_t kuzu_rel_val_get_property_size(kuzu_rel_val* rel_val);
/**
 * @brief Returns the property name of the given rel value at the given index.
 * @param rel_val The rel value to return.
 * @param index The index of the property.
 */
KUZU_C_API char* kuzu_rel_val_get_property_name_at(kuzu_rel_val* rel_val, uint64_t index);
/**
 * @brief Returns the property of the given rel value at the given index as kuzu value.
 * @param rel_val The rel value to return.
 * @param index The index of the property.
 */
KUZU_C_API kuzu_value* kuzu_rel_val_get_property_value_at(kuzu_rel_val* rel_val, uint64_t index);
/**
 * @brief Adds the property with name to the given rel value.
 * @param rel_val The rel value to add to.
 * @param name The name of the property.
 * @param property The property(in value format) to add.
 */
KUZU_C_API void kuzu_rel_val_add_property(
    kuzu_rel_val* rel_val, const char* name, kuzu_value* property);
/**
 * @brief Converts the given rel value to string.
 * @param rel_val The rel value to convert.
 */
KUZU_C_API char* kuzu_rel_val_to_string(kuzu_rel_val* rel_val);

// QuerySummary
/**
 * @brief Destroys the given query summary.
 * @param query_summary The query summary to destroy.
 */
KUZU_C_API void kuzu_query_summary_destroy(kuzu_query_summary* query_summary);
/**
 * @brief Returns the compilation time of the given query summary in milliseconds.
 * @param query_summary The query summary to get compilation time.
 */
KUZU_C_API double kuzu_query_summary_get_compiling_time(kuzu_query_summary* query_summary);
/**
 * @brief Returns the execution time of the given query summary in milliseconds.
 * @param query_summary The query summary to get execution time.
 */
KUZU_C_API double kuzu_query_summary_get_execution_time(kuzu_query_summary* query_summary);

// TODO: Bind utility functions for kuzu_date_t, kuzu_timestamp_t, and kuzu_interval_t
#undef KUZU_C_API
