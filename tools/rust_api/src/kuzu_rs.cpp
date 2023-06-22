#include "kuzu_rs.h"

using kuzu::common::FixedListTypeInfo;
using kuzu::common::Interval;
using kuzu::common::LogicalType;
using kuzu::common::LogicalTypeID;
using kuzu::common::StructField;
using kuzu::common::Value;
using kuzu::common::VarListTypeInfo;
using kuzu::main::Connection;
using kuzu::main::Database;
using kuzu::main::SystemConfig;

namespace kuzu_rs {

std::unique_ptr<QueryParams> new_params() {
    return std::make_unique<QueryParams>();
}

std::unique_ptr<LogicalType> create_logical_type(kuzu::common::LogicalTypeID id) {
    return std::make_unique<LogicalType>(id);
}
std::unique_ptr<LogicalType> create_logical_type_var_list(std::unique_ptr<LogicalType> childType) {
    return std::make_unique<LogicalType>(
        LogicalTypeID::VAR_LIST, std::make_unique<VarListTypeInfo>(std::move(childType)));
}

std::unique_ptr<LogicalType> create_logical_type_fixed_list(
    std::unique_ptr<LogicalType> childType, uint64_t numElements) {
    return std::make_unique<LogicalType>(LogicalTypeID::FIXED_LIST,
        std::make_unique<FixedListTypeInfo>(std::move(childType), numElements));
}

std::unique_ptr<kuzu::common::LogicalType> create_logical_type_struct(
    const rust::Vec<rust::String>& fieldNames, std::unique_ptr<TypeListBuilder> fieldTypes) {
    std::vector<std::unique_ptr<StructField>> fields;
    for (auto i = 0; i < fieldNames.size(); i++) {
        fields.push_back(std::make_unique<StructField>(
            std::string(fieldNames[i]), std::move(fieldTypes->types[i])));
    }
    return std::make_unique<LogicalType>(
        LogicalTypeID::STRUCT, std::make_unique<kuzu::common::StructTypeInfo>(std::move(fields)));
}

const LogicalType& logical_type_get_var_list_child_type(const LogicalType& logicalType) {
    return *kuzu::common::VarListType::getChildType(&logicalType);
}
const LogicalType& logical_type_get_fixed_list_child_type(const LogicalType& logicalType) {
    return *kuzu::common::FixedListType::getChildType(&logicalType);
}
uint64_t logical_type_get_fixed_list_num_elements(const LogicalType& logicalType) {
    return kuzu::common::FixedListType::getNumElementsInList(&logicalType);
}

rust::Vec<rust::String> logical_type_get_struct_field_names(
    const kuzu::common::LogicalType& value) {
    rust::Vec<rust::String> names;
    for (auto name : kuzu::common::StructType::getFieldNames(&value)) {
        names.push_back(name);
    }
    return names;
}

std::unique_ptr<std::vector<kuzu::common::LogicalType>> logical_type_get_struct_field_types(
    const kuzu::common::LogicalType& value) {
    std::vector<kuzu::common::LogicalType> result;
    for (auto type : kuzu::common::StructType::getFieldTypes(&value)) {
        result.push_back(*type);
    }
    return std::make_unique<std::vector<LogicalType>>(result);
}

std::unique_ptr<Database> new_database(const std::string& databasePath, uint64_t bufferPoolSize) {
    auto systemConfig = SystemConfig();
    if (bufferPoolSize > 0) {
        systemConfig.bufferPoolSize = bufferPoolSize;
    }
    return std::make_unique<Database>(databasePath, systemConfig);
}

void database_set_logging_level(Database& database, const std::string& level) {
    database.setLoggingLevel(level);
}

std::unique_ptr<kuzu::main::Connection> database_connect(kuzu::main::Database& database) {
    return std::make_unique<Connection>(&database);
}

std::unique_ptr<kuzu::main::QueryResult> connection_execute(kuzu::main::Connection& connection,
    kuzu::main::PreparedStatement& query, std::unique_ptr<QueryParams> params) {
    return connection.executeWithParams(&query, params->inputParams);
}

rust::String get_node_table_names(Connection& connection) {
    return rust::String(connection.getNodeTableNames());
}
rust::String get_rel_table_names(Connection& connection) {
    return rust::String(connection.getRelTableNames());
}
rust::String get_node_property_names(Connection& connection, rust::Str tableName) {
    return rust::String(connection.getNodePropertyNames(std::string(tableName)));
}
rust::String get_rel_property_names(Connection& connection, rust::Str relTableName) {
    return rust::String(connection.getRelPropertyNames(std::string(relTableName)));
}

rust::String prepared_statement_error_message(const kuzu::main::PreparedStatement& statement) {
    return rust::String(statement.getErrorMessage());
}

rust::String query_result_to_string(kuzu::main::QueryResult& result) {
    return rust::String(result.toString());
}

rust::String query_result_get_error_message(const kuzu::main::QueryResult& result) {
    return rust::String(result.getErrorMessage());
}

double query_result_get_compiling_time(const kuzu::main::QueryResult& result) {
    return result.getQuerySummary()->getCompilingTime();
}
double query_result_get_execution_time(const kuzu::main::QueryResult& result) {
    return result.getQuerySummary()->getExecutionTime();
}

void query_result_write_to_csv(kuzu::main::QueryResult& query_result, const rust::String& filename,
    int8_t delimiter, int8_t escape_character, int8_t newline) {
    query_result.writeToCSV(
        std::string(filename), (char)delimiter, (char)escape_character, (char)newline);
}

std::unique_ptr<std::vector<kuzu::common::LogicalType>> query_result_column_data_types(
    const kuzu::main::QueryResult& query_result) {
    return std::make_unique<std::vector<kuzu::common::LogicalType>>(
        query_result.getColumnDataTypes());
}
rust::Vec<rust::String> query_result_column_names(const kuzu::main::QueryResult& query_result) {
    rust::Vec<rust::String> names;
    for (auto name : query_result.getColumnNames()) {
        names.push_back(name);
    }
    return names;
}

std::array<uint64_t, 2> node_value_get_node_id(const kuzu::common::NodeVal& val) {
    auto internalID = val.getNodeID();
    return std::array{internalID.offset, internalID.tableID};
}

std::array<uint64_t, 2> rel_value_get_src_id(const kuzu::common::RelVal& val) {
    auto internalID = val.getSrcNodeID();
    return std::array{internalID.offset, internalID.tableID};
}
std::array<uint64_t, 2> rel_value_get_dst_id(const kuzu::common::RelVal& val) {
    auto internalID = val.getDstNodeID();
    return std::array{internalID.offset, internalID.tableID};
}

rust::String value_to_string(const kuzu::common::Value& val) {
    return rust::String(val.toString());
}

uint32_t flat_tuple_len(const kuzu::processor::FlatTuple& flatTuple) {
    return flatTuple.len();
}
const kuzu::common::Value& flat_tuple_get_value(
    const kuzu::processor::FlatTuple& flatTuple, uint32_t index) {
    return *flatTuple.getValue(index);
}

rust::String value_get_string(const kuzu::common::Value& value) {
    return value.getValue<std::string>();
}
int64_t value_get_interval_secs(const kuzu::common::Value& value) {
    auto interval = value.getValue<kuzu::common::interval_t>();
    return (interval.months * Interval::DAYS_PER_MONTH + interval.days) * Interval::HOURS_PER_DAY *
               Interval::MINS_PER_HOUR * Interval::SECS_PER_MINUTE
           // Include extra microseconds with the seconds
           + interval.micros / Interval::MICROS_PER_SEC;
}
int32_t value_get_interval_micros(const kuzu::common::Value& value) {
    auto interval = value.getValue<kuzu::common::interval_t>();
    return interval.micros % Interval::MICROS_PER_SEC;
}
int32_t value_get_date_days(const kuzu::common::Value& value) {
    return value.getValue<kuzu::common::date_t>().days;
}
int64_t value_get_timestamp_micros(const kuzu::common::Value& value) {
    return value.getValue<kuzu::common::timestamp_t>().value;
}
std::array<uint64_t, 2> value_get_internal_id(const kuzu::common::Value& value) {
    auto internalID = value.getValue<kuzu::common::internalID_t>();
    return std::array{internalID.offset, internalID.tableID};
}

std::unique_ptr<ValueList> value_get_list(const kuzu::common::Value& value) {
    return std::make_unique<ValueList>(value.getListValReference());
}
kuzu::common::LogicalTypeID value_get_data_type_id(const kuzu::common::Value& value) {
    return value.getDataType().getLogicalTypeID();
}
std::unique_ptr<LogicalType> value_get_data_type(const kuzu::common::Value& value) {
    return std::make_unique<LogicalType>(value.getDataType());
}

std::unique_ptr<kuzu::common::Value> create_value_string(const rust::String& value) {
    return std::make_unique<kuzu::common::Value>(
        LogicalType(LogicalTypeID::STRING), std::string(value));
}
std::unique_ptr<kuzu::common::Value> create_value_timestamp(const int64_t timestamp) {
    return std::make_unique<kuzu::common::Value>(kuzu::common::timestamp_t(timestamp));
}
std::unique_ptr<kuzu::common::Value> create_value_date(const int64_t date) {
    return std::make_unique<kuzu::common::Value>(kuzu::common::date_t(date));
}
std::unique_ptr<kuzu::common::Value> create_value_interval(
    const int32_t months, const int32_t days, const int64_t micros) {
    return std::make_unique<kuzu::common::Value>(kuzu::common::interval_t(months, days, micros));
}
std::unique_ptr<kuzu::common::Value> create_value_null(
    std::unique_ptr<kuzu::common::LogicalType> typ) {
    return std::make_unique<kuzu::common::Value>(
        kuzu::common::Value::createNullValue(kuzu::common::LogicalType(*typ)));
}
std::unique_ptr<kuzu::common::Value> create_value_internal_id(uint64_t offset, uint64_t table) {
    return std::make_unique<kuzu::common::Value>(kuzu::common::internalID_t(offset, table));
}
std::unique_ptr<Value> create_value_node(
    std::unique_ptr<Value> id_val, std::unique_ptr<Value> label_val) {
    return std::make_unique<Value>(
        std::make_unique<kuzu::common::NodeVal>(std::move(id_val), std::move(label_val)));
}

std::unique_ptr<kuzu::common::Value> create_value_rel(std::unique_ptr<kuzu::common::Value> src_id,
    std::unique_ptr<kuzu::common::Value> dst_id, std::unique_ptr<kuzu::common::Value> label_val) {
    return std::make_unique<Value>(std::make_unique<kuzu::common::RelVal>(
        std::move(src_id), std::move(dst_id), std::move(label_val)));
}

std::unique_ptr<kuzu::common::Value> get_list_value(
    std::unique_ptr<kuzu::common::LogicalType> typ, std::unique_ptr<ValueListBuilder> value) {
    return std::make_unique<kuzu::common::Value>(std::move(*typ.get()), std::move(value->values));
}

std::unique_ptr<ValueListBuilder> create_list() {
    return std::make_unique<ValueListBuilder>();
}

std::unique_ptr<TypeListBuilder> create_type_list() {
    return std::make_unique<TypeListBuilder>();
}

void value_add_property(kuzu::common::Value& val, const rust::String& name,
    std::unique_ptr<kuzu::common::Value> property) {
    if (val.getDataType().getLogicalTypeID() == kuzu::common::LogicalTypeID::NODE) {
        kuzu::common::NodeVal& nodeVal = val.getValueReference<kuzu::common::NodeVal>();
        nodeVal.addProperty(std::string(name), std::move(property));
    } else if (val.getDataType().getLogicalTypeID() == kuzu::common::LogicalTypeID::REL) {
        kuzu::common::RelVal& relVal = val.getValueReference<kuzu::common::RelVal>();
        relVal.addProperty(std::string(name), std::move(property));
    } else {
        throw std::runtime_error("Internal Error! Adding property to type without properties!");
    }
}

} // namespace kuzu_rs
