#include "function/cast/vector_cast_operations.h"

#include "common/vector/value_vector_utils.h"
#include "function/cast/cast_operations.h"

namespace kuzu {
namespace function {

vector<unique_ptr<VectorOperationDefinition>> CastToDateVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        make_unique<VectorOperationDefinition>(CAST_TO_DATE_FUNC_NAME, vector<DataTypeID>{STRING},
            DATE, UnaryExecFunction<ku_string_t, date_t, operation::CastStringToDate>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> CastToTimestampVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_TIMESTAMP_FUNC_NAME,
        vector<DataTypeID>{STRING}, TIMESTAMP,
        UnaryExecFunction<ku_string_t, timestamp_t, operation::CastStringToTimestamp>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> CastToIntervalVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_INTERVAL_FUNC_NAME,
        vector<DataTypeID>{STRING}, INTERVAL,
        UnaryExecFunction<ku_string_t, interval_t, operation::CastStringToInterval>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> CastToStringVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME, vector<DataTypeID>{BOOL},
            STRING, UnaryCastExecFunction<bool, ku_string_t, operation::CastToString>));
    result.push_back(
        make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME, vector<DataTypeID>{INT64},
            STRING, UnaryCastExecFunction<int64_t, ku_string_t, operation::CastToString>));
    result.push_back(
        make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME, vector<DataTypeID>{DOUBLE},
            STRING, UnaryCastExecFunction<double_t, ku_string_t, operation::CastToString>));
    result.push_back(
        make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME, vector<DataTypeID>{DATE},
            STRING, UnaryCastExecFunction<date_t, ku_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        vector<DataTypeID>{TIMESTAMP}, STRING,
        UnaryCastExecFunction<timestamp_t, ku_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        vector<DataTypeID>{INTERVAL}, STRING,
        UnaryCastExecFunction<interval_t, ku_string_t, operation::CastToString>));
    result.push_back(
        make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME, vector<DataTypeID>{STRING},
            STRING, UnaryCastExecFunction<ku_string_t, ku_string_t, operation::CastToString>));
    result.push_back(
        make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME, vector<DataTypeID>{LIST},
            STRING, UnaryCastExecFunction<ku_list_t, ku_string_t, operation::CastToString>));
    return result;
}

} // namespace function
} // namespace kuzu
