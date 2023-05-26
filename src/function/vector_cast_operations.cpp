#include "function/cast/vector_cast_operations.h"

#include "common/vector/value_vector_utils.h"
#include "function/cast/cast_operations.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

bool VectorCastOperations::hasImplicitCast(
    const common::LogicalType& srcType, const common::LogicalType& dstType) {
    // We allow cast between any numerical types
    if (LogicalTypeUtils::isNumerical(srcType) && LogicalTypeUtils::isNumerical(dstType)) {
        return true;
    }
    switch (srcType.getLogicalTypeID()) {
    case common::LogicalTypeID::STRING: {
        switch (dstType.getLogicalTypeID()) {
        case common::LogicalTypeID::DATE:
        case common::LogicalTypeID::TIMESTAMP:
        case common::LogicalTypeID::INTERVAL:
            return true;
        default:
            return false;
        }
    }
    default:
        return false;
    }
}

std::string VectorCastOperations::bindImplicitCastFuncName(const common::LogicalType& dstType) {
    switch (dstType.getLogicalTypeID()) {
    case common::LogicalTypeID::INT16:
        return CAST_TO_INT16_FUNC_NAME;
    case common::LogicalTypeID::INT32:
        return CAST_TO_INT32_FUNC_NAME;
    case common::LogicalTypeID::INT64:
        return CAST_TO_INT64_FUNC_NAME;
    case common::LogicalTypeID::FLOAT:
        return CAST_TO_FLOAT_FUNC_NAME;
    case common::LogicalTypeID::DOUBLE:
        return CAST_TO_DOUBLE_FUNC_NAME;
    case common::LogicalTypeID::DATE:
        return CAST_TO_DATE_FUNC_NAME;
    case common::LogicalTypeID::TIMESTAMP:
        return CAST_TO_TIMESTAMP_FUNC_NAME;
    case common::LogicalTypeID::INTERVAL:
        return CAST_TO_INTERVAL_FUNC_NAME;
    case common::LogicalTypeID::STRING:
        return CAST_TO_STRING_FUNC_NAME;
    default:
        throw common::NotImplementedException("bindImplicitCastFuncName()");
    }
}

scalar_exec_func VectorCastOperations::bindImplicitCastFunc(
    common::LogicalTypeID sourceTypeID, common::LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case common::LogicalTypeID::INT16: {
        return bindImplicitNumericalCastFunc<int16_t, operation::CastToInt16>(sourceTypeID);
    }
    case common::LogicalTypeID::INT32: {
        return bindImplicitNumericalCastFunc<int32_t, operation::CastToInt32>(sourceTypeID);
    }
    case common::LogicalTypeID::INT64: {
        return bindImplicitNumericalCastFunc<int64_t, operation::CastToInt64>(sourceTypeID);
    }
    case common::LogicalTypeID::FLOAT: {
        return bindImplicitNumericalCastFunc<float_t, operation::CastToFloat>(sourceTypeID);
    }
    case common::LogicalTypeID::DOUBLE: {
        return bindImplicitNumericalCastFunc<double_t, operation::CastToDouble>(sourceTypeID);
    }
    case common::LogicalTypeID::DATE: {
        assert(sourceTypeID == common::LogicalTypeID::STRING);
        return &UnaryExecFunction<ku_string_t, date_t, operation::CastStringToDate>;
    }
    case common::LogicalTypeID::TIMESTAMP: {
        assert(sourceTypeID == common::LogicalTypeID::STRING);
        return &UnaryExecFunction<ku_string_t, timestamp_t, operation::CastStringToTimestamp>;
    }
    case common::LogicalTypeID::INTERVAL: {
        assert(sourceTypeID == common::LogicalTypeID::STRING);
        return &UnaryExecFunction<ku_string_t, interval_t, operation::CastStringToInterval>;
    }
    default:
        throw common::NotImplementedException(
            "Unimplemented casting operation from " +
            common::LogicalTypeUtils::dataTypeToString(sourceTypeID) + " to " +
            common::LogicalTypeUtils::dataTypeToString(targetTypeID) + ".");
    }
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToDateVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_DATE_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, LogicalTypeID::DATE,
        UnaryExecFunction<ku_string_t, date_t, operation::CastStringToDate>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToTimestampVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(std::make_unique<VectorOperationDefinition>(CAST_TO_TIMESTAMP_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, LogicalTypeID::TIMESTAMP,
        UnaryExecFunction<ku_string_t, timestamp_t, operation::CastStringToTimestamp>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToIntervalVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_INTERVAL_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, LogicalTypeID::INTERVAL,
        UnaryExecFunction<ku_string_t, interval_t, operation::CastStringToInterval>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToStringVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::BOOL}, LogicalTypeID::STRING,
        UnaryCastExecFunction<bool, ku_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INT64}, LogicalTypeID::STRING,
        UnaryCastExecFunction<int64_t, ku_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DOUBLE}, LogicalTypeID::STRING,
        UnaryCastExecFunction<double_t, ku_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE}, LogicalTypeID::STRING,
        UnaryCastExecFunction<date_t, ku_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP}, LogicalTypeID::STRING,
        UnaryCastExecFunction<timestamp_t, ku_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INTERVAL}, LogicalTypeID::STRING,
        UnaryCastExecFunction<interval_t, ku_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, LogicalTypeID::STRING,
        UnaryCastExecFunction<ku_string_t, ku_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST}, LogicalTypeID::STRING,
        UnaryCastExecFunction<list_entry_t, ku_string_t, operation::CastToString>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToDoubleVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(bindVectorOperation<int16_t, double_t, operation::CastToDouble>(
        CAST_TO_DOUBLE_FUNC_NAME, LogicalTypeID::INT16, LogicalTypeID::DOUBLE));
    result.push_back(bindVectorOperation<int32_t, double_t, operation::CastToDouble>(
        CAST_TO_DOUBLE_FUNC_NAME, LogicalTypeID::INT32, LogicalTypeID::DOUBLE));
    result.push_back(bindVectorOperation<int64_t, double_t, operation::CastToDouble>(
        CAST_TO_DOUBLE_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::DOUBLE));
    result.push_back(bindVectorOperation<float_t, double_t, operation::CastToDouble>(
        CAST_TO_DOUBLE_FUNC_NAME, LogicalTypeID::FLOAT, LogicalTypeID::DOUBLE));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToFloatVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(bindVectorOperation<int16_t, float_t, operation::CastToFloat>(
        CAST_TO_FLOAT_FUNC_NAME, LogicalTypeID::INT16, LogicalTypeID::FLOAT));
    result.push_back(bindVectorOperation<int32_t, float_t, operation::CastToFloat>(
        CAST_TO_FLOAT_FUNC_NAME, LogicalTypeID::INT32, LogicalTypeID::FLOAT));
    result.push_back(bindVectorOperation<int64_t, float_t, operation::CastToFloat>(
        CAST_TO_FLOAT_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::FLOAT));
    // down cast
    result.push_back(bindVectorOperation<double_t, float_t, operation::CastToFloat>(
        CAST_TO_FLOAT_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::FLOAT));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToInt64VectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(bindVectorOperation<int16_t, int64_t, operation::CastToInt64>(
        CAST_TO_INT64_FUNC_NAME, LogicalTypeID::INT16, LogicalTypeID::INT64));
    result.push_back(bindVectorOperation<int32_t, int64_t, operation::CastToInt64>(
        CAST_TO_INT64_FUNC_NAME, LogicalTypeID::INT32, LogicalTypeID::INT64));
    // down cast
    result.push_back(bindVectorOperation<float_t, int64_t, operation::CastToInt64>(
        CAST_TO_INT64_FUNC_NAME, LogicalTypeID::FLOAT, LogicalTypeID::INT64));
    result.push_back(bindVectorOperation<double_t, int64_t, operation::CastToInt64>(
        CAST_TO_INT64_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::INT64));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToInt32VectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(bindVectorOperation<int16_t, int32_t, operation::CastToInt32>(
        CAST_TO_INT32_FUNC_NAME, LogicalTypeID::INT16, LogicalTypeID::INT32));
    // down cast
    result.push_back(bindVectorOperation<int64_t, int32_t, operation::CastToInt32>(
        CAST_TO_INT32_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT32));
    result.push_back(bindVectorOperation<float_t, int32_t, operation::CastToInt32>(
        CAST_TO_INT32_FUNC_NAME, LogicalTypeID::FLOAT, LogicalTypeID::INT32));
    result.push_back(bindVectorOperation<double_t, int32_t, operation::CastToInt32>(
        CAST_TO_INT32_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::INT32));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToInt16VectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    // down cast
    result.push_back(bindVectorOperation<int32_t, int16_t, operation::CastToInt16>(
        CAST_TO_INT16_FUNC_NAME, LogicalTypeID::INT32, LogicalTypeID::INT16));
    result.push_back(bindVectorOperation<int64_t, int16_t, operation::CastToInt16>(
        CAST_TO_INT16_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT16));
    result.push_back(bindVectorOperation<float_t, int16_t, operation::CastToInt16>(
        CAST_TO_INT16_FUNC_NAME, LogicalTypeID::FLOAT, LogicalTypeID::INT16));
    result.push_back(bindVectorOperation<double_t, int16_t, operation::CastToInt16>(
        CAST_TO_INT16_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::INT16));
    return result;
}

} // namespace function
} // namespace kuzu
