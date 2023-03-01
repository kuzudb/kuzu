#include "function/cast/vector_cast_operations.h"

#include "common/vector/value_vector_utils.h"
#include "function/cast/cast_operations.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

scalar_exec_func VectorCastOperations::bindImplicitCastFunc(
    common::DataTypeID sourceTypeID, common::DataTypeID targetTypeID) {
    switch (sourceTypeID) {
    case common::INT16: {
        return bindImplicitCastInt16Func(targetTypeID);
    }
    case common::INT32: {
        return bindImplicitCastInt32Func(targetTypeID);
    }
    case common::INT64: {
        return bindImplicitCastInt64Func(targetTypeID);
    }
    case common::FLOAT: {
        return bindImplicitCastFloatFunc(targetTypeID);
    }
    case common::FLOAT: {
        switch (targetTypeID) {
        case common::DOUBLE:
            return VectorOperations::UnaryExecFunction<float_t, double_t, operation::CastToDouble>;
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

scalar_exec_func VectorCastOperations::bindImplicitCastInt16Func(common::DataTypeID targetTypeID) {
    switch (targetTypeID) {
    case common::INT32: {
        return VectorOperations::UnaryExecFunction<int16_t, int32_t, operation::CastToInt32>;
    }
    case common::INT64: {
        return VectorOperations::UnaryExecFunction<int16_t, int64_t, operation::CastToInt64>;
    }
    case common::FLOAT: {
        return VectorOperations::UnaryExecFunction<int16_t, float_t, operation::CastToFloat>;
    }
    case common::DOUBLE: {
        return VectorOperations::UnaryExecFunction<int16_t, double_t, operation::CastToDouble>;
    }
    default: {
        throw common::InternalException("Undefined casting operation from INT16 to " +
                                        common::Types::dataTypeToString(targetTypeID) + ".");
    }
    }
}

scalar_exec_func VectorCastOperations::bindImplicitCastInt32Func(common::DataTypeID targetTypeID) {
    switch (targetTypeID) {
    case common::INT64:
        return VectorOperations::UnaryExecFunction<int32_t, int64_t, operation::CastToInt64>;
    case common::FLOAT:
        return VectorOperations::UnaryExecFunction<int32_t, float_t, operation::CastToFloat>;
    case common::DOUBLE:
        return VectorOperations::UnaryExecFunction<int32_t, double_t, operation::CastToDouble>;
    default:
        throw common::InternalException("Undefined casting operation from INT32 to " +
                                        common::Types::dataTypeToString(targetTypeID) + ".");
    }
}

scalar_exec_func VectorCastOperations::bindImplicitCastInt64Func(common::DataTypeID targetTypeID) {
    switch (targetTypeID) {
    case common::FLOAT:
        return VectorOperations::UnaryExecFunction<int64_t, float_t, operation::CastToFloat>;
    case common::DOUBLE:
        return VectorOperations::UnaryExecFunction<int64_t, double_t, operation::CastToDouble>;
    default:
        throw common::InternalException("Undefined casting operation from INT64 to " +
                                        common::Types::dataTypeToString(targetTypeID) + ".");
    }
}

scalar_exec_func VectorCastOperations::bindImplicitCastFloatFunc(common::DataTypeID targetTypeID) {
    switch (targetTypeID) {
    case common::DOUBLE:
        return VectorOperations::UnaryExecFunction<float_t, double_t, operation::CastToDouble>;
    default:
        throw common::InternalException("Undefined casting operation from FLOAT to " +
                                        common::Types::dataTypeToString(targetTypeID) + ".");
    }
}

std::string VectorCastOperations::bindCastFunctionName(common::DataTypeID targetTypeID) {
    switch (targetTypeID) {
    case common::INT64: {
        return CAST_TO_INT64_FUNC_NAME;
    }
    case common::INT32: {
        return CAST_TO_INT32_FUNC_NAME;
    }
    case common::DOUBLE: {
        return CAST_TO_DOUBLE_FUNC_NAME;
    }
    case common::FLOAT: {
        return CAST_TO_FLOAT_FUNC_NAME;
    }
    default: {
        throw common::InternalException("Cannot bind function name for cast to " +
                                        common::Types::dataTypeToString(targetTypeID));
    }
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
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<DataTypeID>{FLOAT}, STRING,
        UnaryCastExecFunction<float_t, ku_string_t, operation::CastToString>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToDoubleVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
<<<<<<< HEAD
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_DOUBLE_FUNC_NAME,
        std::vector<DataTypeID>{INT64}, DOUBLE, bindExecFunc(INT64, DOUBLE)));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_DOUBLE_FUNC_NAME,
        std::vector<DataTypeID>{FLOAT}, DOUBLE, bindExecFunc(FLOAT, DOUBLE)));
=======
    result.push_back(bindVectorOperation<int16_t, double_t, operation::CastToDouble>(
        CAST_TO_DOUBLE_FUNC_NAME, INT16, DOUBLE));
    result.push_back(bindVectorOperation<int32_t, double_t, operation::CastToDouble>(
        CAST_TO_DOUBLE_FUNC_NAME, INT32, DOUBLE));
    result.push_back(bindVectorOperation<int64_t, double_t, operation::CastToDouble>(
        CAST_TO_DOUBLE_FUNC_NAME, INT64, DOUBLE));
    result.push_back(bindVectorOperation<float_t, double_t, operation::CastToDouble>(
        CAST_TO_DOUBLE_FUNC_NAME, FLOAT, DOUBLE));
>>>>>>> origin/master
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToFloatVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
<<<<<<< HEAD
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_FLOAT_FUNC_NAME,
        std::vector<DataTypeID>{INT64}, FLOAT, bindExecFunc(INT64, FLOAT)));
=======
    result.push_back(bindVectorOperation<int16_t, float_t, operation::CastToFloat>(
        CAST_TO_FLOAT_FUNC_NAME, INT16, FLOAT));
    result.push_back(bindVectorOperation<int32_t, float_t, operation::CastToFloat>(
        CAST_TO_FLOAT_FUNC_NAME, INT32, FLOAT));
    result.push_back(bindVectorOperation<int64_t, float_t, operation::CastToFloat>(
        CAST_TO_FLOAT_FUNC_NAME, INT64, FLOAT));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToInt64VectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(bindVectorOperation<int16_t, int64_t, operation::CastToInt64>(
        CAST_TO_INT64_FUNC_NAME, INT16, INT64));
    result.push_back(bindVectorOperation<int32_t, int64_t, operation::CastToInt64>(
        CAST_TO_INT64_FUNC_NAME, INT32, INT64));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToInt32VectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(bindVectorOperation<int16_t, int32_t, operation::CastToInt32>(
        CAST_TO_INT32_FUNC_NAME, INT16, INT32));
>>>>>>> origin/master
    return result;
}

} // namespace function
} // namespace kuzu
