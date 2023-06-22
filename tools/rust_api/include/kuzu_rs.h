#pragma once
#include <memory>

#include "main/kuzu.h"
#include "rust/cxx.h"
// Need to explicitly import some types.
// The generated C++ wrapper code needs to be able to call sizeof on PreparedStatement,
// which it can't do when it only sees forward declarations of its components.
#include <binder/bound_statement.h>
#include <main/prepared_statement.h>
#include <planner/logical_plan/logical_plan.h>

namespace kuzu_rs {

struct TypeListBuilder {
    std::vector<std::unique_ptr<kuzu::common::LogicalType>> types;

    void insert(std::unique_ptr<kuzu::common::LogicalType> type) {
        types.push_back(std::move(type));
    }
};

std::unique_ptr<TypeListBuilder> create_type_list();

struct QueryParams {
    std::unordered_map<std::string, std::shared_ptr<kuzu::common::Value>> inputParams;

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
    const rust::Vec<rust::String>& fieldNames, std::unique_ptr<TypeListBuilder> fieldTypes);

const kuzu::common::LogicalType& logical_type_get_var_list_child_type(
    const kuzu::common::LogicalType& logicalType);
const kuzu::common::LogicalType& logical_type_get_fixed_list_child_type(
    const kuzu::common::LogicalType& logicalType);
uint64_t logical_type_get_fixed_list_num_elements(const kuzu::common::LogicalType& logicalType);

rust::Vec<rust::String> logical_type_get_struct_field_names(const kuzu::common::LogicalType& value);
std::unique_ptr<std::vector<kuzu::common::LogicalType>> logical_type_get_struct_field_types(
    const kuzu::common::LogicalType& value);

// Simple wrapper for vector of unique_ptr since cxx doesn't support functions returning a vector of
// unique_ptr
struct ValueList {
    ValueList(const std::vector<std::unique_ptr<kuzu::common::Value>>& values) : values(values) {}
    const std::vector<std::unique_ptr<kuzu::common::Value>>& values;
    uint64_t size() const { return values.size(); }
    const std::unique_ptr<kuzu::common::Value>& get(uint64_t index) const { return values[index]; }
};

/* Database */
std::unique_ptr<kuzu::main::Database> new_database(
    const std::string& databasePath, uint64_t bufferPoolSize);

void database_set_logging_level(kuzu::main::Database& database, const std::string& level);

/* Connection */
std::unique_ptr<kuzu::main::Connection> database_connect(kuzu::main::Database& database);
std::unique_ptr<kuzu::main::QueryResult> connection_execute(kuzu::main::Connection& connection,
    kuzu::main::PreparedStatement& query, std::unique_ptr<QueryParams> params);

rust::String get_node_table_names(kuzu::main::Connection& connection);
rust::String get_rel_table_names(kuzu::main::Connection& connection);
rust::String get_node_property_names(kuzu::main::Connection& connection, rust::Str tableName);
rust::String get_rel_property_names(kuzu::main::Connection& connection, rust::Str relTableName);

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
struct PropertyList {
    const std::vector<std::pair<std::string, std::unique_ptr<kuzu::common::Value>>>& properties;

    size_t size() const { return properties.size(); }
    rust::String get_name(size_t index) const {
        return rust::String(this->properties[index].first);
    }
    const kuzu::common::Value& get_value(size_t index) const {
        return *this->properties[index].second.get();
    }
};

template<typename T>
rust::String value_get_label_name(const T& val) {
    return val.getLabelName();
}
template<typename T>
std::unique_ptr<PropertyList> value_get_properties(const T& val) {
    return std::make_unique<PropertyList>(val.getProperties());
}

/* NodeVal */
std::array<uint64_t, 2> node_value_get_node_id(const kuzu::common::NodeVal& val);

/* RelVal */
std::array<uint64_t, 2> rel_value_get_src_id(const kuzu::common::RelVal& val);
std::array<uint64_t, 2> rel_value_get_dst_id(const kuzu::common::RelVal& val);

/* FlatTuple */
const kuzu::common::Value& flat_tuple_get_value(
    const kuzu::processor::FlatTuple& flatTuple, uint32_t index);

/* Value */
rust::String value_get_string(const kuzu::common::Value& value);

template<typename T>
std::unique_ptr<T> value_get_unique(const kuzu::common::Value& value) {
    return std::make_unique<T>(value.getValue<T>());
}

int64_t value_get_interval_secs(const kuzu::common::Value& value);
int32_t value_get_interval_micros(const kuzu::common::Value& value);
int32_t value_get_date_days(const kuzu::common::Value& value);
int64_t value_get_timestamp_micros(const kuzu::common::Value& value);
std::array<uint64_t, 2> value_get_internal_id(const kuzu::common::Value& value);
std::unique_ptr<ValueList> value_get_list(const kuzu::common::Value& value);
kuzu::common::LogicalTypeID value_get_data_type_id(const kuzu::common::Value& value);
std::unique_ptr<kuzu::common::LogicalType> value_get_data_type(const kuzu::common::Value& value);
rust::String value_to_string(const kuzu::common::Value& val);

std::unique_ptr<kuzu::common::Value> create_value_string(const rust::String& value);
std::unique_ptr<kuzu::common::Value> create_value_timestamp(const int64_t timestamp);
std::unique_ptr<kuzu::common::Value> create_value_date(const int64_t date);
std::unique_ptr<kuzu::common::Value> create_value_interval(
    const int32_t months, const int32_t days, const int64_t micros);
std::unique_ptr<kuzu::common::Value> create_value_null(
    std::unique_ptr<kuzu::common::LogicalType> typ);
std::unique_ptr<kuzu::common::Value> create_value_internal_id(uint64_t offset, uint64_t table);
std::unique_ptr<kuzu::common::Value> create_value_node(
    std::unique_ptr<kuzu::common::Value> id_val, std::unique_ptr<kuzu::common::Value> label_val);
std::unique_ptr<kuzu::common::Value> create_value_rel(std::unique_ptr<kuzu::common::Value> src_id,
    std::unique_ptr<kuzu::common::Value> dst_id, std::unique_ptr<kuzu::common::Value> label_val);

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

void value_add_property(kuzu::common::Value& val, const rust::String& name,
    std::unique_ptr<kuzu::common::Value> property);

} // namespace kuzu_rs
