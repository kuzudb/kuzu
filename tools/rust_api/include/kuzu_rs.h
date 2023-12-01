#pragma once

#include <memory>

#include "rust/cxx.h"
#ifdef KUZU_BUNDLED
#include "common/types/value/nested.h"
#include "common/types/value/node.h"
#include "common/types/value/recursive_rel.h"
#include "common/types/value/rel.h"
#include "common/types/value/value.h"
#include "main/kuzu.h"
#else
#include <kuzu.hpp>
#endif

namespace kuzu_rs {

struct TypeListBuilder {
    std::vector<std::unique_ptr<kuzu::common::LogicalType>> types;

    void insert(std::unique_ptr<kuzu::common::LogicalType> type) {
        types.push_back(std::move(type));
    }
};

std::unique_ptr<TypeListBuilder> create_type_list();

struct QueryParams {
    std::unordered_map<std::string, std::unique_ptr<kuzu::common::Value>> inputParams;

    void insert(const rust::Str key, std::unique_ptr<kuzu::common::Value> value) {
        inputParams.insert(std::make_pair(key, std::move(value)));
    }
};

std::unique_ptr<QueryParams> new_params();

std::unique_ptr<kuzu::common::LogicalType> create_logical_type(kuzu::common::LogicalTypeID id);
std::unique_ptr<kuzu::common::LogicalType> create_logical_type_var_list(
    std::unique_ptr<kuzu::common::LogicalType> childType);
std::unique_ptr<kuzu::common::LogicalType> create_logical_type_fixed_list(
    std::unique_ptr<kuzu::common::LogicalType> childType, uint64_t numElements);
std::unique_ptr<kuzu::common::LogicalType> create_logical_type_struct(
    kuzu::common::LogicalTypeID typeID, const rust::Vec<rust::String>& fieldNames,
    std::unique_ptr<TypeListBuilder> fieldTypes);
std::unique_ptr<kuzu::common::LogicalType> create_logical_type_map(
    std::unique_ptr<kuzu::common::LogicalType> keyType,
    std::unique_ptr<kuzu::common::LogicalType> valueType);

const kuzu::common::LogicalType& logical_type_get_var_list_child_type(
    const kuzu::common::LogicalType& logicalType);
const kuzu::common::LogicalType& logical_type_get_fixed_list_child_type(
    const kuzu::common::LogicalType& logicalType);
uint64_t logical_type_get_fixed_list_num_elements(const kuzu::common::LogicalType& logicalType);

rust::Vec<rust::String> logical_type_get_struct_field_names(const kuzu::common::LogicalType& value);
std::unique_ptr<std::vector<kuzu::common::LogicalType>> logical_type_get_struct_field_types(
    const kuzu::common::LogicalType& value);

/* Database */
std::unique_ptr<kuzu::main::Database> new_database(const std::string& databasePath,
    uint64_t bufferPoolSize, uint64_t maxNumThreads, bool enableCompression, bool readOnly);

void database_set_logging_level(kuzu::main::Database& database, const std::string& level);

/* Connection */
std::unique_ptr<kuzu::main::Connection> database_connect(kuzu::main::Database& database);
std::unique_ptr<kuzu::main::QueryResult> connection_execute(kuzu::main::Connection& connection,
    kuzu::main::PreparedStatement& query, std::unique_ptr<QueryParams> params);

/* PreparedStatement */
rust::String prepared_statement_error_message(const kuzu::main::PreparedStatement& statement);

/* QueryResult */
rust::String query_result_to_string(kuzu::main::QueryResult& result);
rust::String query_result_get_error_message(const kuzu::main::QueryResult& result);

double query_result_get_compiling_time(const kuzu::main::QueryResult& result);
double query_result_get_execution_time(const kuzu::main::QueryResult& result);

void query_result_write_to_csv(kuzu::main::QueryResult& query_result, const rust::String& filename,
    int8_t delimiter, int8_t escape_character, int8_t newline);

std::unique_ptr<std::vector<kuzu::common::LogicalType>> query_result_column_data_types(
    const kuzu::main::QueryResult& query_result);
rust::Vec<rust::String> query_result_column_names(const kuzu::main::QueryResult& query_result);

/* NodeVal/RelVal */
rust::String node_value_get_label_name(const kuzu::common::Value& val);
rust::String rel_value_get_label_name(const kuzu::common::Value& val);

size_t node_value_get_num_properties(const kuzu::common::Value& value);
size_t rel_value_get_num_properties(const kuzu::common::Value& value);

rust::String node_value_get_property_name(const kuzu::common::Value& value, size_t index);
rust::String rel_value_get_property_name(const kuzu::common::Value& value, size_t index);

const kuzu::common::Value& node_value_get_property_value(
    const kuzu::common::Value& value, size_t index);
const kuzu::common::Value& rel_value_get_property_value(
    const kuzu::common::Value& value, size_t index);

/* NodeVal */
const kuzu::common::Value& node_value_get_node_id(const kuzu::common::Value& val);

/* RelVal */
const kuzu::common::Value& rel_value_get_src_id(const kuzu::common::Value& val);
std::array<uint64_t, 2> rel_value_get_dst_id(const kuzu::common::Value& val);

/* RecursiveRel */
const kuzu::common::Value& recursive_rel_get_nodes(const kuzu::common::Value& val);
const kuzu::common::Value& recursive_rel_get_rels(const kuzu::common::Value& val);

/* FlatTuple */
const kuzu::common::Value& flat_tuple_get_value(
    const kuzu::processor::FlatTuple& flatTuple, uint32_t index);

/* Value */
const std::string& value_get_string(const kuzu::common::Value& value);

template<typename T>
std::unique_ptr<T> value_get_unique(const kuzu::common::Value& value) {
    return std::make_unique<T>(value.getValue<T>());
}

int64_t value_get_interval_secs(const kuzu::common::Value& value);
int32_t value_get_interval_micros(const kuzu::common::Value& value);
int32_t value_get_date_days(const kuzu::common::Value& value);
int64_t value_get_timestamp_micros(const kuzu::common::Value& value);
std::array<uint64_t, 2> value_get_int128_t(const kuzu::common::Value& value);
std::array<uint64_t, 2> value_get_internal_id(const kuzu::common::Value& value);
uint32_t value_get_children_size(const kuzu::common::Value& value);
const kuzu::common::Value& value_get_child(const kuzu::common::Value& value, uint32_t index);
kuzu::common::LogicalTypeID value_get_data_type_id(const kuzu::common::Value& value);
const kuzu::common::LogicalType& value_get_data_type(const kuzu::common::Value& value);
rust::String value_to_string(const kuzu::common::Value& val);

std::unique_ptr<kuzu::common::Value> create_value_string(
    kuzu::common::LogicalTypeID typ, const rust::Slice<const unsigned char> value);
std::unique_ptr<kuzu::common::Value> create_value_timestamp(const int64_t timestamp);
std::unique_ptr<kuzu::common::Value> create_value_date(const int64_t date);
std::unique_ptr<kuzu::common::Value> create_value_interval(
    const int32_t months, const int32_t days, const int64_t micros);
std::unique_ptr<kuzu::common::Value> create_value_null(
    std::unique_ptr<kuzu::common::LogicalType> typ);
std::unique_ptr<kuzu::common::Value> create_value_int128_t(int64_t high, uint64_t low);
std::unique_ptr<kuzu::common::Value> create_value_internal_id(uint64_t offset, uint64_t table);

template<typename T>
std::unique_ptr<kuzu::common::Value> create_value(const T value) {
    return std::make_unique<kuzu::common::Value>(value);
}

struct ValueListBuilder {
    std::vector<std::unique_ptr<kuzu::common::Value>> values;

    void insert(std::unique_ptr<kuzu::common::Value> value) { values.push_back(std::move(value)); }
};

std::unique_ptr<kuzu::common::Value> get_list_value(
    std::unique_ptr<kuzu::common::LogicalType> typ, std::unique_ptr<ValueListBuilder> value);
std::unique_ptr<ValueListBuilder> create_list();

} // namespace kuzu_rs
