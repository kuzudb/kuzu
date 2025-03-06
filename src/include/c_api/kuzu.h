#pragma once
#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#ifdef _WIN32
#include <windows.h>
#endif

/* Export header from common/api.h */
// Helpers
#if defined _WIN32 || defined __CYGWIN__
#define KUZU_HELPER_DLL_IMPORT __declspec(dllimport)
#define KUZU_HELPER_DLL_EXPORT __declspec(dllexport)
#define KUZU_HELPER_DLL_LOCAL
#define KUZU_HELPER_DEPRECATED __declspec(deprecated)
#else
#define KUZU_HELPER_DLL_IMPORT __attribute__((visibility("default")))
#define KUZU_HELPER_DLL_EXPORT __attribute__((visibility("default")))
#define KUZU_HELPER_DLL_LOCAL __attribute__((visibility("hidden")))
#define KUZU_HELPER_DEPRECATED __attribute__((__deprecated__))
#endif

#ifdef KUZU_STATIC_DEFINE
#define KUZU_API
#define KUZU_NO_EXPORT
#else
#ifndef KUZU_API
#ifdef KUZU_EXPORTS
/* We are building this library */
#define KUZU_API KUZU_HELPER_DLL_EXPORT
#else
/* We are using this library */
#define KUZU_API KUZU_HELPER_DLL_IMPORT
#endif
#endif

#endif

#ifndef KUZU_DEPRECATED
#define KUZU_DEPRECATED KUZU_HELPER_DEPRECATED
#endif

#ifndef KUZU_DEPRECATED_EXPORT
#define KUZU_DEPRECATED_EXPORT KUZU_API KUZU_DEPRECATED
#endif
/* end export header */

// The Arrow C data interface.
// https://arrow.apache.org/docs/format/CDataInterface.html

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
    // Array type description
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;

    // Release callback
    void (*release)(struct ArrowSchema*);
    // Opaque producer-specific data
    void* private_data;
};

struct ArrowArray {
    // Array data description
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;

    // Release callback
    void (*release)(struct ArrowArray*);
    // Opaque producer-specific data
    void* private_data;
};

#endif // ARROW_C_DATA_INTERFACE

#ifdef __cplusplus
}
#endif

#ifdef __cplusplus
#define KUZU_C_API extern "C" KUZU_API
#else
#define KUZU_C_API KUZU_API
#endif

/**
 * @brief Stores runtime configuration for creating or opening a Database
 */
typedef struct {
    // bufferPoolSize Max size of the buffer pool in bytes.
    // The larger the buffer pool, the more data from the database files is kept in memory,
    // reducing the amount of File I/O
    uint64_t buffer_pool_size;
    // The maximum number of threads to use during query execution
    uint64_t max_num_threads;
    // Whether or not to compress data on-disk for supported types
    bool enable_compression;
    // If true, open the database in read-only mode. No write transaction is allowed on the Database
    // object. If false, open the database read-write.
    bool read_only;
    //  The maximum size of the database in bytes. Note that this is introduced temporarily for now
    //  to get around with the default 8TB mmap address space limit under some environment. This
    //  will be removed once we implemente a better solution later. The value is default to 1 << 43
    //  (8TB) under 64-bit environment and 1GB under 32-bit one (see `DEFAULT_VM_REGION_MAX_SIZE`).
    uint64_t max_db_size;
    // If true, the database will automatically checkpoint when the size of
    // the WAL file exceeds the checkpoint threshold.
    bool auto_checkpoint;
    // The threshold of the WAL file size in bytes. When the size of the
    // WAL file exceeds this threshold, the database will checkpoint if auto_checkpoint is true.
    uint64_t checkpoint_threshold;
} kuzu_system_config;

/**
 * @brief kuzu_database manages all database components.
 */
typedef struct {
    void* _database;
} kuzu_database;

/**
 * @brief kuzu_connection is used to interact with a Database instance. Each connection is
 * thread-safe. Multiple connections can connect to the same Database instance in a multi-threaded
 * environment.
 */
typedef struct {
    void* _connection;
} kuzu_connection;

/**
 * @brief kuzu_prepared_statement is a parameterized query which can avoid planning the same query
 * for repeated execution.
 */
typedef struct {
    void* _prepared_statement;
    void* _bound_values;
} kuzu_prepared_statement;

/**
 * @brief kuzu_query_result stores the result of a query.
 */
typedef struct {
    void* _query_result;
    bool _is_owned_by_cpp;
} kuzu_query_result;

/**
 * @brief kuzu_flat_tuple stores a vector of values.
 */
typedef struct {
    void* _flat_tuple;
    bool _is_owned_by_cpp;
} kuzu_flat_tuple;

/**
 * @brief kuzu_logical_type is the kuzu internal representation of data types.
 */
typedef struct {
    void* _data_type;
} kuzu_logical_type;

/**
 * @brief kuzu_value is used to represent a value with any kuzu internal dataType.
 */
typedef struct {
    void* _value;
    bool _is_owned_by_cpp;
} kuzu_value;

/**
 * @brief kuzu internal internal_id type which stores the table_id and offset of a node/rel.
 */
typedef struct {
    uint64_t table_id;
    uint64_t offset;
} kuzu_internal_id_t;

/**
 * @brief kuzu internal date type which stores the number of days since 1970-01-01 00:00:00 UTC.
 */
typedef struct {
    // Days since 1970-01-01 00:00:00 UTC.
    int32_t days;
} kuzu_date_t;

/**
 * @brief kuzu internal timestamp_ns type which stores the number of nanoseconds since 1970-01-01
 * 00:00:00 UTC.
 */
typedef struct {
    // Nanoseconds since 1970-01-01 00:00:00 UTC.
    int64_t value;
} kuzu_timestamp_ns_t;

/**
 * @brief kuzu internal timestamp_ms type which stores the number of milliseconds since 1970-01-01
 * 00:00:00 UTC.
 */
typedef struct {
    // Milliseconds since 1970-01-01 00:00:00 UTC.
    int64_t value;
} kuzu_timestamp_ms_t;

/**
 * @brief kuzu internal timestamp_sec_t type which stores the number of seconds since 1970-01-01
 * 00:00:00 UTC.
 */
typedef struct {
    // Seconds since 1970-01-01 00:00:00 UTC.
    int64_t value;
} kuzu_timestamp_sec_t;

/**
 * @brief kuzu internal timestamp_tz type which stores the number of microseconds since 1970-01-01
 * with timezone 00:00:00 UTC.
 */
typedef struct {
    // Microseconds since 1970-01-01 00:00:00 UTC.
    int64_t value;
} kuzu_timestamp_tz_t;

/**
 * @brief kuzu internal timestamp type which stores the number of microseconds since 1970-01-01
 * 00:00:00 UTC.
 */
typedef struct {
    // Microseconds since 1970-01-01 00:00:00 UTC.
    int64_t value;
} kuzu_timestamp_t;

/**
 * @brief kuzu internal interval type which stores the months, days and microseconds.
 */
typedef struct {
    int32_t months;
    int32_t days;
    int64_t micros;
} kuzu_interval_t;

/**
 * @brief kuzu_query_summary stores the execution time, plan, compiling time and query options of a
 * query.
 */
typedef struct {
    void* _query_summary;
} kuzu_query_summary;

typedef struct {
    uint64_t low;
    int64_t high;
} kuzu_int128_t;

/**
 * @brief enum class for kuzu internal dataTypes.
 */
typedef enum {
    KUZU_ANY = 0,
    KUZU_NODE = 10,
    KUZU_REL = 11,
    KUZU_RECURSIVE_REL = 12,
    // SERIAL is a special data type that is used to represent a sequence of INT64 values that are
    // incremented by 1 starting from 0.
    KUZU_SERIAL = 13,
    // fixed size types
    KUZU_BOOL = 22,
    KUZU_INT64 = 23,
    KUZU_INT32 = 24,
    KUZU_INT16 = 25,
    KUZU_INT8 = 26,
    KUZU_UINT64 = 27,
    KUZU_UINT32 = 28,
    KUZU_UINT16 = 29,
    KUZU_UINT8 = 30,
    KUZU_INT128 = 31,
    KUZU_DOUBLE = 32,
    KUZU_FLOAT = 33,
    KUZU_DATE = 34,
    KUZU_TIMESTAMP = 35,
    KUZU_TIMESTAMP_SEC = 36,
    KUZU_TIMESTAMP_MS = 37,
    KUZU_TIMESTAMP_NS = 38,
    KUZU_TIMESTAMP_TZ = 39,
    KUZU_INTERVAL = 40,
    KUZU_DECIMAL = 41,
    KUZU_INTERNAL_ID = 42,
    // variable size types
    KUZU_STRING = 50,
    KUZU_BLOB = 51,
    KUZU_LIST = 52,
    KUZU_ARRAY = 53,
    KUZU_STRUCT = 54,
    KUZU_MAP = 55,
    KUZU_UNION = 56,
    KUZU_POINTER = 58,
    KUZU_UUID = 59
} kuzu_data_type_id;

/**
 * @brief enum class for kuzu function return state.
 */
typedef enum { KuzuSuccess = 0, KuzuError = 1 } kuzu_state;

// Database
/**
 * @brief Allocates memory and creates a kuzu database instance at database_path with
 * bufferPoolSize=buffer_pool_size. Caller is responsible for calling kuzu_database_destroy() to
 * release the allocated memory.
 * @param database_path The path to the database.
 * @param system_config The runtime configuration for creating or opening the database.
 * @param[out] out_database The output parameter that will hold the database instance.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_database_init(const char* database_path,
    kuzu_system_config system_config, kuzu_database* out_database);
/**
 * @brief Destroys the kuzu database instance and frees the allocated memory.
 * @param database The database instance to destroy.
 */
KUZU_C_API void kuzu_database_destroy(kuzu_database* database);

KUZU_C_API kuzu_system_config kuzu_default_system_config();

// Connection
/**
 * @brief Allocates memory and creates a connection to the database. Caller is responsible for
 * calling kuzu_connection_destroy() to release the allocated memory.
 * @param database The database instance to connect to.
 * @param[out] out_connection The output parameter that will hold the connection instance.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_connection_init(kuzu_database* database,
    kuzu_connection* out_connection);
/**
 * @brief Destroys the connection instance and frees the allocated memory.
 * @param connection The connection instance to destroy.
 */
KUZU_C_API void kuzu_connection_destroy(kuzu_connection* connection);
/**
 * @brief Sets the maximum number of threads to use for executing queries.
 * @param connection The connection instance to set max number of threads for execution.
 * @param num_threads The maximum number of threads to use for executing queries.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_connection_set_max_num_thread_for_exec(kuzu_connection* connection,
    uint64_t num_threads);

/**
 * @brief Returns the maximum number of threads of the connection to use for executing queries.
 * @param connection The connection instance to return max number of threads for execution.
 * @param[out] out_result The output parameter that will hold the maximum number of threads to use
 * for executing queries.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_connection_get_max_num_thread_for_exec(kuzu_connection* connection,
    uint64_t* out_result);
/**
 * @brief Executes the given query and returns the result.
 * @param connection The connection instance to execute the query.
 * @param query The query to execute.
 * @param[out] out_query_result The output parameter that will hold the result of the query.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_connection_query(kuzu_connection* connection, const char* query,
    kuzu_query_result* out_query_result);
/**
 * @brief Prepares the given query and returns the prepared statement.
 * @param connection The connection instance to prepare the query.
 * @param query The query to prepare.
 * @param[out] out_prepared_statement The output parameter that will hold the prepared statement.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_connection_prepare(kuzu_connection* connection, const char* query,
    kuzu_prepared_statement* out_prepared_statement);
/**
 * @brief Executes the prepared_statement using connection.
 * @param connection The connection instance to execute the prepared_statement.
 * @param prepared_statement The prepared statement to execute.
 * @param[out] out_query_result The output parameter that will hold the result of the query.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_connection_execute(kuzu_connection* connection,
    kuzu_prepared_statement* prepared_statement, kuzu_query_result* out_query_result);
/**
 * @brief Interrupts the current query execution in the connection.
 * @param connection The connection instance to interrupt.
 */
KUZU_C_API void kuzu_connection_interrupt(kuzu_connection* connection);
/**
 * @brief Sets query timeout value in milliseconds for the connection.
 * @param connection The connection instance to set query timeout value.
 * @param timeout_in_ms The timeout value in milliseconds.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_connection_set_query_timeout(kuzu_connection* connection,
    uint64_t timeout_in_ms);

// PreparedStatement
/**
 * @brief Destroys the prepared statement instance and frees the allocated memory.
 * @param prepared_statement The prepared statement instance to destroy.
 */
KUZU_C_API void kuzu_prepared_statement_destroy(kuzu_prepared_statement* prepared_statement);
/**
 * @return the query is prepared successfully or not.
 */
KUZU_C_API bool kuzu_prepared_statement_is_success(kuzu_prepared_statement* prepared_statement);
/**
 * @param prepared_statement The prepared statement instance.
 * @return the error message if the statement is not prepared successfully.
 */
KUZU_C_API char* kuzu_prepared_statement_get_error_message(
    kuzu_prepared_statement* prepared_statement);
/**
 * @brief Binds the given boolean value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The boolean value to bind.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_prepared_statement_bind_bool(kuzu_prepared_statement* prepared_statement,
    const char* param_name, bool value);
/**
 * @brief Binds the given int64_t value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The int64_t value to bind.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_prepared_statement_bind_int64(
    kuzu_prepared_statement* prepared_statement, const char* param_name, int64_t value);
/**
 * @brief Binds the given int32_t value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The int32_t value to bind.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_prepared_statement_bind_int32(
    kuzu_prepared_statement* prepared_statement, const char* param_name, int32_t value);
/**
 * @brief Binds the given int16_t value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The int16_t value to bind.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_prepared_statement_bind_int16(
    kuzu_prepared_statement* prepared_statement, const char* param_name, int16_t value);
/**
 * @brief Binds the given int8_t value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The int8_t value to bind.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_prepared_statement_bind_int8(kuzu_prepared_statement* prepared_statement,
    const char* param_name, int8_t value);
/**
 * @brief Binds the given uint64_t value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The uint64_t value to bind.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_prepared_statement_bind_uint64(
    kuzu_prepared_statement* prepared_statement, const char* param_name, uint64_t value);
/**
 * @brief Binds the given uint32_t value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The uint32_t value to bind.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_prepared_statement_bind_uint32(
    kuzu_prepared_statement* prepared_statement, const char* param_name, uint32_t value);
/**
 * @brief Binds the given uint16_t value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The uint16_t value to bind.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_prepared_statement_bind_uint16(
    kuzu_prepared_statement* prepared_statement, const char* param_name, uint16_t value);
/**
 * @brief Binds the given int8_t value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The int8_t value to bind.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_prepared_statement_bind_uint8(
    kuzu_prepared_statement* prepared_statement, const char* param_name, uint8_t value);

/**
 * @brief Binds the given double value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The double value to bind.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_prepared_statement_bind_double(
    kuzu_prepared_statement* prepared_statement, const char* param_name, double value);
/**
 * @brief Binds the given float value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The float value to bind.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_prepared_statement_bind_float(
    kuzu_prepared_statement* prepared_statement, const char* param_name, float value);
/**
 * @brief Binds the given date value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The date value to bind.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_prepared_statement_bind_date(kuzu_prepared_statement* prepared_statement,
    const char* param_name, kuzu_date_t value);
/**
 * @brief Binds the given timestamp_ns value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The timestamp_ns value to bind.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_prepared_statement_bind_timestamp_ns(
    kuzu_prepared_statement* prepared_statement, const char* param_name, kuzu_timestamp_ns_t value);
/**
 * @brief Binds the given timestamp_sec value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The timestamp_sec value to bind.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_prepared_statement_bind_timestamp_sec(
    kuzu_prepared_statement* prepared_statement, const char* param_name,
    kuzu_timestamp_sec_t value);
/**
 * @brief Binds the given timestamp_tz value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The timestamp_tz value to bind.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_prepared_statement_bind_timestamp_tz(
    kuzu_prepared_statement* prepared_statement, const char* param_name, kuzu_timestamp_tz_t value);
/**
 * @brief Binds the given timestamp_ms value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The timestamp_ms value to bind.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_prepared_statement_bind_timestamp_ms(
    kuzu_prepared_statement* prepared_statement, const char* param_name, kuzu_timestamp_ms_t value);
/**
 * @brief Binds the given timestamp value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The timestamp value to bind.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_prepared_statement_bind_timestamp(
    kuzu_prepared_statement* prepared_statement, const char* param_name, kuzu_timestamp_t value);
/**
 * @brief Binds the given interval value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The interval value to bind.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_prepared_statement_bind_interval(
    kuzu_prepared_statement* prepared_statement, const char* param_name, kuzu_interval_t value);
/**
 * @brief Binds the given string value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The string value to bind.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_prepared_statement_bind_string(
    kuzu_prepared_statement* prepared_statement, const char* param_name, const char* value);
/**
 * @brief Binds the given kuzu value to the given parameter name in the prepared statement.
 * @param prepared_statement The prepared statement instance to bind the value.
 * @param param_name The parameter name to bind the value.
 * @param value The kuzu value to bind.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_prepared_statement_bind_value(
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
 * @return The error message if the query has failed.
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
 * @param[out] out_column_name The output parameter that will hold the column name.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_query_result_get_column_name(kuzu_query_result* query_result,
    uint64_t index, char** out_column_name);
/**
 * @brief Returns the data type of the column at the given index.
 * @param query_result The query result instance to return.
 * @param index The index of the column to return data type.
 * @param[out] out_column_data_type The output parameter that will hold the column data type.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_query_result_get_column_data_type(kuzu_query_result* query_result,
    uint64_t index, kuzu_logical_type* out_column_data_type);
/**
 * @brief Returns the number of tuples in the query result.
 * @param query_result The query result instance to return.
 */
KUZU_C_API uint64_t kuzu_query_result_get_num_tuples(kuzu_query_result* query_result);
/**
 * @brief Returns the query summary of the query result.
 * @param query_result The query result instance to return.
 * @param[out] out_query_summary The output parameter that will hold the query summary.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_query_result_get_query_summary(kuzu_query_result* query_result,
    kuzu_query_summary* out_query_summary);
/**
 * @brief Returns true if we have not consumed all tuples in the query result, false otherwise.
 * @param query_result The query result instance to check.
 */
KUZU_C_API bool kuzu_query_result_has_next(kuzu_query_result* query_result);
/**
 * @brief Returns the next tuple in the query result. Throws an exception if there is no more tuple.
 * Note that to reduce resource allocation, all calls to kuzu_query_result_get_next() reuse the same
 * FlatTuple object. Since its contents will be overwritten, please complete processing a FlatTuple
 * or make a copy of its data before calling kuzu_query_result_get_next() again.
 * @param query_result The query result instance to return.
 * @param[out] out_flat_tuple The output parameter that will hold the next tuple.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_query_result_get_next(kuzu_query_result* query_result,
    kuzu_flat_tuple* out_flat_tuple);
/**
 * @brief Returns true if we have not consumed all query results, false otherwise. Use this function
 * for loop results of multiple query statements
 * @param query_result The query result instance to check.
 */
KUZU_C_API bool kuzu_query_result_has_next_query_result(kuzu_query_result* query_result);
/**
 * @brief Returns the next query result. Use this function to loop multiple query statements'
 * results.
 * @param query_result The query result instance to return.
 * @param[out] out_next_query_result The output parameter that will hold the next query result.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_query_result_get_next_query_result(kuzu_query_result* query_result,
    kuzu_query_result* out_next_query_result);

/**
 * @brief Returns the query result as a string.
 * @param query_result The query result instance to return.
 * @return The query result as a string.
 */
KUZU_C_API char* kuzu_query_result_to_string(kuzu_query_result* query_result);
/**
 * @brief Resets the iterator of the query result to the beginning of the query result.
 * @param query_result The query result instance to reset iterator.
 */
KUZU_C_API void kuzu_query_result_reset_iterator(kuzu_query_result* query_result);

/**
 * @brief Returns the query result's schema as ArrowSchema.
 * @param query_result The query result instance to return.
 * @param[out] out_schema The output parameter that will hold the datatypes of the columns as an
 * arrow schema.
 * @return The state indicating the success or failure of the operation.
 *
 * It is the caller's responsibility to call the release function to release the underlying data
 */
KUZU_C_API kuzu_state kuzu_query_result_get_arrow_schema(kuzu_query_result* query_result,
    struct ArrowSchema* out_schema);

/**
 * @brief Returns the next chunk of the query result as ArrowArray.
 * @param query_result The query result instance to return.
 * @param chunk_size The number of tuples to return in the chunk.
 * @param[out] out_arrow_array The output parameter that will hold the arrow array representation of
 * the query result. The arrow array internally stores an arrow struct with fields for each of the
 * columns.
 * @return The state indicating the success or failure of the operation.
 *
 * It is the caller's responsibility to call the release function to release the underlying data
 */
KUZU_C_API kuzu_state kuzu_query_result_get_next_arrow_chunk(kuzu_query_result* query_result,
    int64_t chunk_size, struct ArrowArray* out_arrow_array);

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
 * @param[out] out_value The output parameter that will hold the value at index.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_flat_tuple_get_value(kuzu_flat_tuple* flat_tuple, uint64_t index,
    kuzu_value* out_value);
/**
 * @brief Converts the flat tuple to a string.
 * @param flat_tuple The flat tuple instance to convert.
 * @return The flat tuple as a string.
 */
KUZU_C_API char* kuzu_flat_tuple_to_string(kuzu_flat_tuple* flat_tuple);

// DataType
// TODO(Chang): Refactor the datatype constructor to follow the cpp way of creating dataTypes.
/**
 * @brief Creates a data type instance with the given id, childType and num_elements_in_array.
 * Caller is responsible for destroying the returned data type instance.
 * @param id The enum type id of the datatype to create.
 * @param child_type The child type of the datatype to create(only used for nested dataTypes).
 * @param num_elements_in_array The number of elements in the array(only used for ARRAY).
 * @param[out] out_type The output parameter that will hold the data type instance.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API void kuzu_data_type_create(kuzu_data_type_id id, kuzu_logical_type* child_type,
    uint64_t num_elements_in_array, kuzu_logical_type* out_type);
/**
 * @brief Creates a new data type instance by cloning the given data type instance.
 * @param data_type The data type instance to clone.
 * @param[out] out_type The output parameter that will hold the cloned data type instance.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API void kuzu_data_type_clone(kuzu_logical_type* data_type, kuzu_logical_type* out_type);
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
 * @brief Returns the number of elements for array.
 * @param data_type The data type instance to return.
 * @param[out] out_result The output parameter that will hold the number of elements in the array.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_data_type_get_num_elements_in_array(kuzu_logical_type* data_type,
    uint64_t* out_result);

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
 * @brief Creates a value with int8 type and the given int8 value. Caller is responsible for
 * destroying the returned value.
 * @param val_ The int8 value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_int8(int8_t val_);
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
 * @brief Creates a value with uint8 type and the given uint8 value. Caller is responsible for
 * destroying the returned value.
 * @param val_ The uint8 value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_uint8(uint8_t val_);
/**
 * @brief Creates a value with uint16 type and the given uint16 value. Caller is responsible for
 * destroying the returned value.
 * @param val_ The uint16 value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_uint16(uint16_t val_);
/**
 * @brief Creates a value with uint32 type and the given uint32 value. Caller is responsible for
 * destroying the returned value.
 * @param val_ The uint32 value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_uint32(uint32_t val_);
/**
 * @brief Creates a value with uint64 type and the given uint64 value. Caller is responsible for
 * destroying the returned value.
 * @param val_ The uint64 value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_uint64(uint64_t val_);
/**
 * @brief Creates a value with int128 type and the given int128 value. Caller is responsible for
 * destroying the returned value.
 * @param val_ The int128 value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_int128(kuzu_int128_t val_);
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
 * @brief Creates a value with date type and the given date value. Caller is responsible for
 * destroying the returned value.
 * @param val_ The date value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_date(kuzu_date_t val_);
/**
 * @brief Creates a value with timestamp_ns type and the given timestamp value. Caller is
 * responsible for destroying the returned value.
 * @param val_ The timestamp_ns value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_timestamp_ns(kuzu_timestamp_ns_t val_);
/**
 * @brief Creates a value with timestamp_ms type and the given timestamp value. Caller is
 * responsible for destroying the returned value.
 * @param val_ The timestamp_ms value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_timestamp_ms(kuzu_timestamp_ms_t val_);
/**
 * @brief Creates a value with timestamp_sec type and the given timestamp value. Caller is
 * responsible for destroying the returned value.
 * @param val_ The timestamp_sec value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_timestamp_sec(kuzu_timestamp_sec_t val_);
/**
 * @brief Creates a value with timestamp_tz type and the given timestamp value. Caller is
 * responsible for destroying the returned value.
 * @param val_ The timestamp_tz value of the value to create.
 */
KUZU_C_API kuzu_value* kuzu_value_create_timestamp_tz(kuzu_timestamp_tz_t val_);
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
 * @brief Creates a list value with the given number of elements and the given elements.
 * The caller needs to make sure that all elements have the same type.
 * The elements are copied into the list value, so destroying the elements after creating the list
 * value is safe.
 * Caller is responsible for destroying the returned value.
 * @param num_elements The number of elements in the list.
 * @param elements The elements of the list.
 * @param[out] out_value The output parameter that will hold a pointer to the created list value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_create_list(uint64_t num_elements, kuzu_value** elements,
    kuzu_value** out_value);
/**
 * @brief Creates a struct value with the given number of fields and the given field names and
 * values. The caller needs to make sure that all field names are unique.
 * The field names and values are copied into the struct value, so destroying the field names and
 * values after creating the struct value is safe.
 * Caller is responsible for destroying the returned value.
 * @param num_fields The number of fields in the struct.
 * @param field_names The field names of the struct.
 * @param field_values The field values of the struct.
 * @param[out] out_value The output parameter that will hold a pointer to the created struct value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_create_struct(uint64_t num_fields, const char** field_names,
    kuzu_value** field_values, kuzu_value** out_value);
/**
 * @brief Creates a map value with the given number of fields and the given keys and values. The
 * caller needs to make sure that all keys are unique, and all keys and values have the same type.
 * The keys and values are copied into the map value, so destroying the keys and values after
 * creating the map value is safe.
 * Caller is responsible for destroying the returned value.
 * @param num_fields The number of fields in the map.
 * @param keys The keys of the map.
 * @param values The values of the map.
 * @param[out] out_value The output parameter that will hold a pointer to the created map value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_create_map(uint64_t num_fields, kuzu_value** keys,
    kuzu_value** values, kuzu_value** out_value);
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
 * ARRAY.
 * @param value The ARRAY value to get list size.
 * @param[out] out_result The output parameter that will hold the number of elements per list.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_list_size(kuzu_value* value, uint64_t* out_result);
/**
 * @brief Returns the element at index of the given value. The value must be of type LIST.
 * @param value The LIST value to return.
 * @param index The index of the element to return.
 * @param[out] out_value The output parameter that will hold the element at index.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_list_element(kuzu_value* value, uint64_t index,
    kuzu_value* out_value);
/**
 * @brief Returns the number of fields of the given struct value. The value must be of type STRUCT.
 * @param value The STRUCT value to get number of fields.
 * @param[out] out_result The output parameter that will hold the number of fields.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_struct_num_fields(kuzu_value* value, uint64_t* out_result);
/**
 * @brief Returns the field name at index of the given struct value. The value must be of physical
 * type STRUCT (STRUCT, NODE, REL, RECURSIVE_REL, UNION).
 * @param value The STRUCT value to get field name.
 * @param index The index of the field name to return.
 * @param[out] out_result The output parameter that will hold the field name at index.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_struct_field_name(kuzu_value* value, uint64_t index,
    char** out_result);
/**
 * @brief Returns the field value at index of the given struct value. The value must be of physical
 * type STRUCT (STRUCT, NODE, REL, RECURSIVE_REL, UNION).
 * @param value The STRUCT value to get field value.
 * @param index The index of the field value to return.
 * @param[out] out_value The output parameter that will hold the field value at index.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_struct_field_value(kuzu_value* value, uint64_t index,
    kuzu_value* out_value);

/**
 * @brief Returns the size of the given map value. The value must be of type MAP.
 * @param value The MAP value to get size.
 * @param[out] out_result The output parameter that will hold the size of the map.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_map_size(kuzu_value* value, uint64_t* out_result);
/**
 * @brief Returns the key at index of the given map value. The value must be of physical
 * type MAP.
 * @param value The MAP value to get key.
 * @param index The index of the field name to return.
 * @param[out] out_key The output parameter that will hold the key at index.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_map_key(kuzu_value* value, uint64_t index,
    kuzu_value* out_key);
/**
 * @brief Returns the field value at index of the given map value. The value must be of physical
 * type MAP.
 * @param value The MAP value to get field value.
 * @param index The index of the field value to return.
 * @param[out] out_value The output parameter that will hold the field value at index.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_map_value(kuzu_value* value, uint64_t index,
    kuzu_value* out_value);
/**
 * @brief Returns the list of nodes for recursive rel value. The value must be of type
 * RECURSIVE_REL.
 * @param value The RECURSIVE_REL value to return.
 * @param[out] out_value The output parameter that will hold the list of nodes.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_recursive_rel_node_list(kuzu_value* value,
    kuzu_value* out_value);

/**
 * @brief Returns the list of rels for recursive rel value. The value must be of type RECURSIVE_REL.
 * @param value The RECURSIVE_REL value to return.
 * @param[out] out_value The output parameter that will hold the list of rels.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_recursive_rel_rel_list(kuzu_value* value,
    kuzu_value* out_value);
/**
 * @brief Returns internal type of the given value.
 * @param value The value to return.
 * @param[out] out_type The output parameter that will hold the internal type of the value.
 */
KUZU_C_API void kuzu_value_get_data_type(kuzu_value* value, kuzu_logical_type* out_type);
/**
 * @brief Returns the boolean value of the given value. The value must be of type BOOL.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the boolean value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_bool(kuzu_value* value, bool* out_result);
/**
 * @brief Returns the int8 value of the given value. The value must be of type INT8.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the int8 value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_int8(kuzu_value* value, int8_t* out_result);
/**
 * @brief Returns the int16 value of the given value. The value must be of type INT16.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the int16 value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_int16(kuzu_value* value, int16_t* out_result);
/**
 * @brief Returns the int32 value of the given value. The value must be of type INT32.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the int32 value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_int32(kuzu_value* value, int32_t* out_result);
/**
 * @brief Returns the int64 value of the given value. The value must be of type INT64 or SERIAL.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the int64 value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_int64(kuzu_value* value, int64_t* out_result);
/**
 * @brief Returns the uint8 value of the given value. The value must be of type UINT8.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the uint8 value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_uint8(kuzu_value* value, uint8_t* out_result);
/**
 * @brief Returns the uint16 value of the given value. The value must be of type UINT16.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the uint16 value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_uint16(kuzu_value* value, uint16_t* out_result);
/**
 * @brief Returns the uint32 value of the given value. The value must be of type UINT32.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the uint32 value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_uint32(kuzu_value* value, uint32_t* out_result);
/**
 * @brief Returns the uint64 value of the given value. The value must be of type UINT64.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the uint64 value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_uint64(kuzu_value* value, uint64_t* out_result);
/**
 * @brief Returns the int128 value of the given value. The value must be of type INT128.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the int128 value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_int128(kuzu_value* value, kuzu_int128_t* out_result);
/**
 * @brief convert a string to int128 value.
 * @param str The string to convert.
 * @param[out] out_result The output parameter that will hold the int128 value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_int128_t_from_string(const char* str, kuzu_int128_t* out_result);
/**
 * @brief convert int128 to corresponding string.
 * @param val The int128 value to convert.
 * @param[out] out_result The output parameter that will hold the string value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_int128_t_to_string(kuzu_int128_t val, char** out_result);
/**
 * @brief Returns the float value of the given value. The value must be of type FLOAT.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the float value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_float(kuzu_value* value, float* out_result);
/**
 * @brief Returns the double value of the given value. The value must be of type DOUBLE.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the double value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_double(kuzu_value* value, double* out_result);
/**
 * @brief Returns the internal id value of the given value. The value must be of type INTERNAL_ID.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the internal id value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_internal_id(kuzu_value* value, kuzu_internal_id_t* out_result);
/**
 * @brief Returns the date value of the given value. The value must be of type DATE.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the date value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_date(kuzu_value* value, kuzu_date_t* out_result);
/**
 * @brief Returns the timestamp value of the given value. The value must be of type TIMESTAMP.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the timestamp value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_timestamp(kuzu_value* value, kuzu_timestamp_t* out_result);
/**
 * @brief Returns the timestamp_ns value of the given value. The value must be of type TIMESTAMP_NS.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the timestamp_ns value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_timestamp_ns(kuzu_value* value,
    kuzu_timestamp_ns_t* out_result);
/**
 * @brief Returns the timestamp_ms value of the given value. The value must be of type TIMESTAMP_MS.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the timestamp_ms value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_timestamp_ms(kuzu_value* value,
    kuzu_timestamp_ms_t* out_result);
/**
 * @brief Returns the timestamp_sec value of the given value. The value must be of type
 * TIMESTAMP_SEC.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the timestamp_sec value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_timestamp_sec(kuzu_value* value,
    kuzu_timestamp_sec_t* out_result);
/**
 * @brief Returns the timestamp_tz value of the given value. The value must be of type TIMESTAMP_TZ.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the timestamp_tz value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_timestamp_tz(kuzu_value* value,
    kuzu_timestamp_tz_t* out_result);
/**
 * @brief Returns the interval value of the given value. The value must be of type INTERVAL.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the interval value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_interval(kuzu_value* value, kuzu_interval_t* out_result);
/**
 * @brief Returns the decimal value of the given value as a string. The value must be of type
 * DECIMAL.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the decimal value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_decimal_as_string(kuzu_value* value, char** out_result);
/**
 * @brief Returns the string value of the given value. The value must be of type STRING.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the string value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_string(kuzu_value* value, char** out_result);
/**
 * @brief Returns the blob value of the given value. The returned buffer is null-terminated similar
 * to a string. The value must be of type BLOB.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the blob value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_blob(kuzu_value* value, uint8_t** out_result);
/**
 * @brief Returns the uuid value of the given value.
 * to a string. The value must be of type UUID.
 * @param value The value to return.
 * @param[out] out_result The output parameter that will hold the uuid value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_value_get_uuid(kuzu_value* value, char** out_result);
/**
 * @brief Converts the given value to string.
 * @param value The value to convert.
 * @return The value as a string.
 */
KUZU_C_API char* kuzu_value_to_string(kuzu_value* value);
/**
 * @brief Returns the internal id value of the given node value as a kuzu value.
 * @param node_val The node value to return.
 * @param[out] out_value The output parameter that will hold the internal id value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_node_val_get_id_val(kuzu_value* node_val, kuzu_value* out_value);
/**
 * @brief Returns the label value of the given node value as a label value.
 * @param node_val The node value to return.
 * @param[out] out_value The output parameter that will hold the label value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_node_val_get_label_val(kuzu_value* node_val, kuzu_value* out_value);
/**
 * @brief Returns the number of properties of the given node value.
 * @param node_val The node value to return.
 * @param[out] out_value The output parameter that will hold the number of properties.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_node_val_get_property_size(kuzu_value* node_val, uint64_t* out_value);
/**
 * @brief Returns the property name of the given node value at the given index.
 * @param node_val The node value to return.
 * @param index The index of the property.
 * @param[out] out_result The output parameter that will hold the property name at index.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_node_val_get_property_name_at(kuzu_value* node_val, uint64_t index,
    char** out_result);
/**
 * @brief Returns the property value of the given node value at the given index.
 * @param node_val The node value to return.
 * @param index The index of the property.
 * @param[out] out_value The output parameter that will hold the property value at index.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_node_val_get_property_value_at(kuzu_value* node_val, uint64_t index,
    kuzu_value* out_value);
/**
 * @brief Converts the given node value to string.
 * @param node_val The node value to convert.
 * @param[out] out_result The output parameter that will hold the node value as a string.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_node_val_to_string(kuzu_value* node_val, char** out_result);
/**
 * @brief Returns the internal id value of the source node of the given rel value as a kuzu value.
 * @param rel_val The rel value to return.
 * @param[out] out_value The output parameter that will hold the internal id value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_rel_val_get_src_id_val(kuzu_value* rel_val, kuzu_value* out_value);
/**
 * @brief Returns the internal id value of the destination node of the given rel value as a kuzu
 * value.
 * @param rel_val The rel value to return.
 * @param[out] out_value The output parameter that will hold the internal id value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_rel_val_get_dst_id_val(kuzu_value* rel_val, kuzu_value* out_value);
/**
 * @brief Returns the label value of the given rel value.
 * @param rel_val The rel value to return.
 * @param[out] out_value The output parameter that will hold the label value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_rel_val_get_label_val(kuzu_value* rel_val, kuzu_value* out_value);
/**
 * @brief Returns the number of properties of the given rel value.
 * @param rel_val The rel value to return.
 * @param[out] out_value The output parameter that will hold the number of properties.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_rel_val_get_property_size(kuzu_value* rel_val, uint64_t* out_value);
/**
 * @brief Returns the property name of the given rel value at the given index.
 * @param rel_val The rel value to return.
 * @param index The index of the property.
 * @param[out] out_result The output parameter that will hold the property name at index.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_rel_val_get_property_name_at(kuzu_value* rel_val, uint64_t index,
    char** out_result);
/**
 * @brief Returns the property of the given rel value at the given index as kuzu value.
 * @param rel_val The rel value to return.
 * @param index The index of the property.
 * @param[out] out_value The output parameter that will hold the property value at index.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_rel_val_get_property_value_at(kuzu_value* rel_val, uint64_t index,
    kuzu_value* out_value);
/**
 * @brief Converts the given rel value to string.
 * @param rel_val The rel value to convert.
 * @param[out] out_result The output parameter that will hold the rel value as a string.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_rel_val_to_string(kuzu_value* rel_val, char** out_result);
/**
 * @brief Destroys any string created by the Kuzu C API, including both the error message and the
 * values returned by the API functions. This function is provided to avoid the inconsistency
 * between the memory allocation and deallocation across different libraries and is preferred over
 * using the standard C free function.
 * @param str The string to destroy.
 */
KUZU_C_API void kuzu_destroy_string(char* str);
/**
 * @brief Destroys any blob created by the Kuzu C API. This function is provided to avoid the
 * inconsistency between the memory allocation and deallocation across different libraries and
 * is preferred over using the standard C free function.
 * @param blob The blob to destroy.
 */
KUZU_C_API void kuzu_destroy_blob(uint8_t* blob);

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

// Utility functions
/**
 * @brief Convert timestamp_ns to corresponding tm struct.
 * @param timestamp The timestamp_ns value to convert.
 * @param[out] out_result The output parameter that will hold the tm struct.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_timestamp_ns_to_tm(kuzu_timestamp_ns_t timestamp, struct tm* out_result);
/**
 * @brief Convert timestamp_ms to corresponding tm struct.
 * @param timestamp The timestamp_ms value to convert.
 * @param[out] out_result The output parameter that will hold the tm struct.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_timestamp_ms_to_tm(kuzu_timestamp_ms_t timestamp, struct tm* out_result);
/**
 * @brief Convert timestamp_sec to corresponding tm struct.
 * @param timestamp The timestamp_sec value to convert.
 * @param[out] out_result The output parameter that will hold the tm struct.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_timestamp_sec_to_tm(kuzu_timestamp_sec_t timestamp,
    struct tm* out_result);
/**
 * @brief Convert timestamp_tz to corresponding tm struct.
 * @param timestamp The timestamp_tz value to convert.
 * @param[out] out_result The output parameter that will hold the tm struct.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_timestamp_tz_to_tm(kuzu_timestamp_tz_t timestamp, struct tm* out_result);
/**
 * @brief Convert timestamp to corresponding tm struct.
 * @param timestamp The timestamp value to convert.
 * @param[out] out_result The output parameter that will hold the tm struct.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_timestamp_to_tm(kuzu_timestamp_t timestamp, struct tm* out_result);
/**
 * @brief Convert tm struct to timestamp_ns value.
 * @param tm The tm struct to convert.
 * @param[out] out_result The output parameter that will hold the timestamp_ns value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_timestamp_ns_from_tm(struct tm tm, kuzu_timestamp_ns_t* out_result);
/**
 * @brief Convert tm struct to timestamp_ms value.
 * @param tm The tm struct to convert.
 * @param[out] out_result The output parameter that will hold the timestamp_ms value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_timestamp_ms_from_tm(struct tm tm, kuzu_timestamp_ms_t* out_result);
/**
 * @brief Convert tm struct to timestamp_sec value.
 * @param tm The tm struct to convert.
 * @param[out] out_result The output parameter that will hold the timestamp_sec value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_timestamp_sec_from_tm(struct tm tm, kuzu_timestamp_sec_t* out_result);
/**
 * @brief Convert tm struct to timestamp_tz value.
 * @param tm The tm struct to convert.
 * @param[out] out_result The output parameter that will hold the timestamp_tz value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_timestamp_tz_from_tm(struct tm tm, kuzu_timestamp_tz_t* out_result);
/**
 * @brief Convert timestamp_ns to corresponding string.
 * @param timestamp The timestamp_ns value to convert.
 * @param[out] out_result The output parameter that will hold the string value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_timestamp_from_tm(struct tm tm, kuzu_timestamp_t* out_result);
/**
 * @brief Convert date to corresponding string.
 * @param date The date value to convert.
 * @param[out] out_result The output parameter that will hold the string value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_date_to_string(kuzu_date_t date, char** out_result);
/**
 * @brief Convert a string to date value.
 * @param str The string to convert.
 * @param[out] out_result The output parameter that will hold the date value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_date_from_string(const char* str, kuzu_date_t* out_result);
/**
 * @brief Convert date to corresponding tm struct.
 * @param date The date value to convert.
 * @param[out] out_result The output parameter that will hold the tm struct.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_date_to_tm(kuzu_date_t date, struct tm* out_result);
/**
 * @brief Convert tm struct to date value.
 * @param tm The tm struct to convert.
 * @param[out] out_result The output parameter that will hold the date value.
 * @return The state indicating the success or failure of the operation.
 */
KUZU_C_API kuzu_state kuzu_date_from_tm(struct tm tm, kuzu_date_t* out_result);
/**
 * @brief Convert interval to corresponding difftime value in seconds.
 * @param interval The interval value to convert.
 * @param[out] out_result The output parameter that will hold the difftime value.
 */
KUZU_C_API void kuzu_interval_to_difftime(kuzu_interval_t interval, double* out_result);
/**
 * @brief Convert difftime value in seconds to interval.
 * @param difftime The difftime value to convert.
 * @param[out] out_result The output parameter that will hold the interval value.
 */
KUZU_C_API void kuzu_interval_from_difftime(double difftime, kuzu_interval_t* out_result);

// Version
/**
 * @brief Returns the version of the Kuzu library.
 */
KUZU_C_API char* kuzu_get_version();

/**
 * @brief Returns the storage version of the Kuzu library.
 */
KUZU_C_API uint64_t kuzu_get_storage_version();
#undef KUZU_C_API
