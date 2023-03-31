#include "function/cast/vector_cast_operations.h"

#include "common/vector/value_vector_utils.h"
#include "function/cast/cast_operations.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

bool VectorCastOperations::hasImplicitCast(
    const common::DataType& srcType, const common::DataType& dstType) {
    // We allow cast between any numerical types
    if (Types::isNumerical(srcType) && Types::isNumerical(dstType)) {
        return true;
    }
    switch (srcType.typeID) {
    case common::STRING: {
        switch (dstType.typeID) {
        case common::DATE:
        case common::TIMESTAMP:
        case common::INTERVAL:
            return true;
        default:
            return false;
        }
    }
    default:
        return false;
    }
}

std::string VectorCastOperations::bindImplicitCastFuncName(const common::DataType& dstType) {
    switch (dstType.typeID) {
    case common::INT16:
        return CAST_TO_INT16_FUNC_NAME;
    case common::INT32:
        return CAST_TO_INT32_FUNC_NAME;
    case common::INT64:
        return CAST_TO_INT64_FUNC_NAME;
    case common::FLOAT:
        return CAST_TO_FLOAT_FUNC_NAME;
    case common::DOUBLE:
        return CAST_TO_DOUBLE_FUNC_NAME;
    case common::DATE:
        return CAST_TO_DATE_FUNC_NAME;
    case common::TIMESTAMP:
        return CAST_TO_TIMESTAMP_FUNC_NAME;
    case common::INTERVAL:
        return CAST_TO_INTERVAL_FUNC_NAME;
    case common::STRING:
        return CAST_TO_STRING_FUNC_NAME;
    default:
        throw common::NotImplementedException("bindImplicitCastFuncName()");
    }
}

scalar_exec_func VectorCastOperations::bindImplicitCastFunc(
    common::DataTypeID sourceTypeID, common::DataTypeID targetTypeID) {
    switch (targetTypeID) {
    case common::INT16: {
        return bindImplicitNumericalCastFunc<int16_t, operation::CastToInt16>(sourceTypeID);
    }
    case common::INT32: {
        return bindImplicitNumericalCastFunc<int32_t, operation::CastToInt32>(sourceTypeID);
    }
    case common::INT64: {
        return bindImplicitNumericalCastFunc<int64_t, operation::CastToInt64>(sourceTypeID);
    }
    case common::FLOAT: {
        return bindImplicitNumericalCastFunc<float_t, operation::CastToFloat>(sourceTypeID);
    }
    case common::DOUBLE: {
        return bindImplicitNumericalCastFunc<double_t, operation::CastToDouble>(sourceTypeID);
    }
    case common::DATE: {
        assert(sourceTypeID == common::STRING);
        return VectorOperations::UnaryExecFunction<ku_string_t, date_t,
            operation::CastStringToDate>;
    }
    case common::TIMESTAMP: {
        assert(sourceTypeID == common::STRING);
        return VectorOperations::UnaryExecFunction<ku_string_t, timestamp_t,
            operation::CastStringToTimestamp>;
    }
    case common::INTERVAL: {
        assert(sourceTypeID == common::STRING);
        return VectorOperations::UnaryExecFunction<ku_string_t, interval_t,
            operation::CastStringToInterval>;
    }
    default:
        throw common::NotImplementedException("Unimplemented casting operation from " +
                                              common::Types::dataTypeToString(sourceTypeID) +
                                              " to " +
                                              common::Types::dataTypeToString(targetTypeID) + ".");
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
    result.push_back(bindVectorOperation<int16_t, double_t, operation::CastToDouble>(
        CAST_TO_DOUBLE_FUNC_NAME, INT16, DOUBLE));
    result.push_back(bindVectorOperation<int32_t, double_t, operation::CastToDouble>(
        CAST_TO_DOUBLE_FUNC_NAME, INT32, DOUBLE));
    result.push_back(bindVectorOperation<int64_t, double_t, operation::CastToDouble>(
        CAST_TO_DOUBLE_FUNC_NAME, INT64, DOUBLE));
    result.push_back(bindVectorOperation<float_t, double_t, operation::CastToDouble>(
        CAST_TO_DOUBLE_FUNC_NAME, FLOAT, DOUBLE));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToFloatVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(bindVectorOperation<int16_t, float_t, operation::CastToFloat>(
        CAST_TO_FLOAT_FUNC_NAME, INT16, FLOAT));
    result.push_back(bindVectorOperation<int32_t, float_t, operation::CastToFloat>(
        CAST_TO_FLOAT_FUNC_NAME, INT32, FLOAT));
    result.push_back(bindVectorOperation<int64_t, float_t, operation::CastToFloat>(
        CAST_TO_FLOAT_FUNC_NAME, INT64, FLOAT));
    // down cast
    result.push_back(bindVectorOperation<double_t, float_t, operation::CastToFloat>(
        CAST_TO_FLOAT_FUNC_NAME, DOUBLE, FLOAT));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToInt64VectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(bindVectorOperation<int16_t, int64_t, operation::CastToInt64>(
        CAST_TO_INT64_FUNC_NAME, INT16, INT64));
    result.push_back(bindVectorOperation<int32_t, int64_t, operation::CastToInt64>(
        CAST_TO_INT64_FUNC_NAME, INT32, INT64));
    // down cast
    result.push_back(bindVectorOperation<float_t, int64_t, operation::CastToInt64>(
        CAST_TO_INT64_FUNC_NAME, FLOAT, INT64));
    result.push_back(bindVectorOperation<double_t, int64_t, operation::CastToInt64>(
        CAST_TO_INT64_FUNC_NAME, DOUBLE, INT64));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToInt32VectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(bindVectorOperation<int16_t, int32_t, operation::CastToInt32>(
        CAST_TO_INT32_FUNC_NAME, INT16, INT32));
    // down cast
    result.push_back(bindVectorOperation<int64_t, int32_t, operation::CastToInt32>(
        CAST_TO_INT32_FUNC_NAME, INT64, INT32));
    result.push_back(bindVectorOperation<float_t, int32_t, operation::CastToInt32>(
        CAST_TO_INT32_FUNC_NAME, FLOAT, INT32));
    result.push_back(bindVectorOperation<double_t, int32_t, operation::CastToInt32>(
        CAST_TO_INT32_FUNC_NAME, DOUBLE, INT32));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToInt16VectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    // down cast
    result.push_back(bindVectorOperation<int32_t, int16_t, operation::CastToInt16>(
        CAST_TO_INT32_FUNC_NAME, INT32, INT16));
    result.push_back(bindVectorOperation<int64_t, int16_t, operation::CastToInt16>(
        CAST_TO_INT32_FUNC_NAME, INT64, INT16));
    result.push_back(bindVectorOperation<float_t, int16_t, operation::CastToInt16>(
        CAST_TO_INT32_FUNC_NAME, FLOAT, INT16));
    result.push_back(bindVectorOperation<double_t, int16_t, operation::CastToInt16>(
        CAST_TO_INT32_FUNC_NAME, DOUBLE, INT16));
    return result;
}

} // namespace function
} // namespace kuzu
