#include "function/cast/vector_cast_operations.h"

#include "common/vector/value_vector_utils.h"
#include "function/cast/cast_operations.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

scalar_exec_func VectorCastOperations::bindExecFunc(
    common::DataTypeID sourceTypeID, common::DataTypeID targetTypeID) {
    switch (sourceTypeID) {
    case common::INT64: {
        switch (targetTypeID) {
        case common::DOUBLE:
            return VectorOperations::UnaryExecFunction<int64_t, double_t, operation::CastToDouble>;
        default:
            throw common::InternalException("Undefined casting operation from " +
                                            common::Types::dataTypeToString(sourceTypeID) + " to " +
                                            common::Types::dataTypeToString(targetTypeID) + ".");
        }
    }
    default:
        throw common::InternalException("Undefined casting operation from " +
                                        common::Types::dataTypeToString(sourceTypeID) + " to " +
                                        common::Types::dataTypeToString(targetTypeID) + ".");
    }
}

std::string VectorCastOperations::bindCastFunctionName(common::DataTypeID targetTypeID) {
    switch (targetTypeID) {
    case common::DOUBLE: {
        return CAST_TO_DOUBLE_FUNC_NAME;
    }
    default:
        throw common::InternalException("Cannot bind function name for cast to " +
                                        common::Types::dataTypeToString(targetTypeID));
    }
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToDateVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_DATE_FUNC_NAME,
        std::vector<DataTypeID>{STRING}, DATE,
        UnaryExecFunction<ku_string_t, date_t, operation::CastStringToDate>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToTimestampVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_TIMESTAMP_FUNC_NAME,
        std::vector<DataTypeID>{STRING}, TIMESTAMP,
        UnaryExecFunction<ku_string_t, timestamp_t, operation::CastStringToTimestamp>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToIntervalVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_INTERVAL_FUNC_NAME,
        std::vector<DataTypeID>{STRING}, INTERVAL,
        UnaryExecFunction<ku_string_t, interval_t, operation::CastStringToInterval>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToStringVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<DataTypeID>{BOOL}, STRING,
        UnaryCastExecFunction<bool, ku_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<DataTypeID>{INT64}, STRING,
        UnaryCastExecFunction<int64_t, ku_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<DataTypeID>{DOUBLE}, STRING,
        UnaryCastExecFunction<double_t, ku_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<DataTypeID>{DATE}, STRING,
        UnaryCastExecFunction<date_t, ku_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<DataTypeID>{TIMESTAMP}, STRING,
        UnaryCastExecFunction<timestamp_t, ku_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<DataTypeID>{INTERVAL}, STRING,
        UnaryCastExecFunction<interval_t, ku_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<DataTypeID>{STRING}, STRING,
        UnaryCastExecFunction<ku_string_t, ku_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<DataTypeID>{VAR_LIST}, STRING,
        UnaryCastExecFunction<ku_list_t, ku_string_t, operation::CastToString>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToDoubleVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_DOUBLE_FUNC_NAME,
        std::vector<DataTypeID>{INT64}, DOUBLE, bindExecFunc(INT64, DOUBLE)));
    return result;
}

} // namespace function
} // namespace kuzu
