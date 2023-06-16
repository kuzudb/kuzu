#include "binder/bound_statement.h"
#include "c_api/kuzu.h"
#include "common/types/value.h"
#include "main/kuzu.h"
#include "planner/logical_plan/logical_plan.h"

using namespace kuzu::common;
using namespace kuzu::main;

void kuzu_prepared_statement_bind_cpp_value(kuzu_prepared_statement* prepared_statement,
    const char* param_name, const std::shared_ptr<Value>& value) {
    auto* bound_values = static_cast<std::unordered_map<std::string, std::shared_ptr<Value>>*>(
        prepared_statement->_bound_values);
    bound_values->insert({param_name, value});
}

void kuzu_prepared_statement_destroy(kuzu_prepared_statement* prepared_statement) {
    if (prepared_statement == nullptr) {
        return;
    }
    if (prepared_statement->_prepared_statement != nullptr) {
        delete static_cast<PreparedStatement*>(prepared_statement->_prepared_statement);
    }
    if (prepared_statement->_bound_values != nullptr) {
        delete static_cast<std::unordered_map<std::string, std::shared_ptr<Value>>*>(
            prepared_statement->_bound_values);
    }
    delete prepared_statement;
}

bool kuzu_prepared_statement_allow_active_transaction(kuzu_prepared_statement* prepared_statement) {
    return static_cast<PreparedStatement*>(prepared_statement->_prepared_statement)
        ->allowActiveTransaction();
}

bool kuzu_prepared_statement_is_success(kuzu_prepared_statement* prepared_statement) {
    return static_cast<PreparedStatement*>(prepared_statement->_prepared_statement)->isSuccess();
}

char* kuzu_prepared_statement_get_error_message(kuzu_prepared_statement* prepared_statement) {
    auto error_message =
        static_cast<PreparedStatement*>(prepared_statement->_prepared_statement)->getErrorMessage();
    if (error_message.empty()) {
        return nullptr;
    }
    char* error_message_c = (char*)malloc(error_message.size() + 1);
    strcpy(error_message_c, error_message.c_str());
    return error_message_c;
}

void kuzu_prepared_statement_bind_bool(
    kuzu_prepared_statement* prepared_statement, const char* param_name, bool value) {
    auto value_ptr = std::make_shared<Value>(value);
    kuzu_prepared_statement_bind_cpp_value(prepared_statement, param_name, value_ptr);
}

void kuzu_prepared_statement_bind_int64(
    kuzu_prepared_statement* prepared_statement, const char* param_name, int64_t value) {
    auto value_ptr = std::make_shared<Value>(value);
    kuzu_prepared_statement_bind_cpp_value(prepared_statement, param_name, value_ptr);
}

void kuzu_prepared_statement_bind_int32(
    kuzu_prepared_statement* prepared_statement, const char* param_name, int32_t value) {
    auto value_ptr = std::make_shared<Value>(value);
    kuzu_prepared_statement_bind_cpp_value(prepared_statement, param_name, value_ptr);
}

void kuzu_prepared_statement_bind_int16(
    kuzu_prepared_statement* prepared_statement, const char* param_name, int16_t value) {
    auto value_ptr = std::make_shared<Value>(value);
    kuzu_prepared_statement_bind_cpp_value(prepared_statement, param_name, value_ptr);
}

void kuzu_prepared_statement_bind_double(
    kuzu_prepared_statement* prepared_statement, const char* param_name, double value) {
    auto value_ptr = std::make_shared<Value>(value);
    kuzu_prepared_statement_bind_cpp_value(prepared_statement, param_name, value_ptr);
}

void kuzu_prepared_statement_bind_float(
    kuzu_prepared_statement* prepared_statement, const char* param_name, float value) {
    auto value_ptr = std::make_shared<Value>(value);
    kuzu_prepared_statement_bind_cpp_value(prepared_statement, param_name, value_ptr);
}

void kuzu_prepared_statement_bind_date(
    kuzu_prepared_statement* prepared_statement, const char* param_name, kuzu_date_t value) {
    auto value_ptr = std::make_shared<Value>(date_t(value.days));
    kuzu_prepared_statement_bind_cpp_value(prepared_statement, param_name, value_ptr);
}

void kuzu_prepared_statement_bind_timestamp(
    kuzu_prepared_statement* prepared_statement, const char* param_name, kuzu_timestamp_t value) {
    auto value_ptr = std::make_shared<Value>(timestamp_t(value.value));
    kuzu_prepared_statement_bind_cpp_value(prepared_statement, param_name, value_ptr);
}

void kuzu_prepared_statement_bind_interval(
    kuzu_prepared_statement* prepared_statement, const char* param_name, kuzu_interval_t value) {
    auto value_ptr = std::make_shared<Value>(interval_t(value.months, value.days, value.micros));
    kuzu_prepared_statement_bind_cpp_value(prepared_statement, param_name, value_ptr);
}

void kuzu_prepared_statement_bind_string(
    kuzu_prepared_statement* prepared_statement, const char* param_name, const char* value) {
    auto value_ptr =
        std::make_shared<Value>(LogicalType{LogicalTypeID::STRING}, std::string(value));
    kuzu_prepared_statement_bind_cpp_value(prepared_statement, param_name, value_ptr);
}

void kuzu_prepared_statement_bind_value(
    kuzu_prepared_statement* prepared_statement, const char* param_name, kuzu_value* value) {
    auto value_ptr = std::make_shared<Value>(*static_cast<Value*>(value->_value));
    kuzu_prepared_statement_bind_cpp_value(prepared_statement, param_name, value_ptr);
}
