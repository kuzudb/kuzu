#include "function/cast/vector_cast_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

bool CastFunction::hasImplicitCast(const LogicalType& srcType, const LogicalType& dstType) {
    // We allow cast between any numerical types
    if (LogicalTypeUtils::isNumerical(srcType) && LogicalTypeUtils::isNumerical(dstType)) {
        return true;
    }
    switch (srcType.getLogicalTypeID()) {
    case LogicalTypeID::DATE: {
        switch (dstType.getLogicalTypeID()) {
        case LogicalTypeID::TIMESTAMP:
            return true;
        default:
            return false;
        }
    }
    case LogicalTypeID::STRING: {
        switch (dstType.getLogicalTypeID()) {
        case LogicalTypeID::DATE:
        case LogicalTypeID::TIMESTAMP:
        case LogicalTypeID::INTERVAL:
            return true;
        default:
            return false;
        }
    }
    default:
        return false;
    }
}

std::string CastFunction::bindImplicitCastFuncName(const LogicalType& dstType) {
    switch (dstType.getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
        return CAST_TO_SERIAL_FUNC_NAME;
    case LogicalTypeID::INT64:
        return CAST_TO_INT64_FUNC_NAME;
    case LogicalTypeID::INT32:
        return CAST_TO_INT32_FUNC_NAME;
    case LogicalTypeID::INT16:
        return CAST_TO_INT16_FUNC_NAME;
    case LogicalTypeID::INT8:
        return CAST_TO_INT8_FUNC_NAME;
    case LogicalTypeID::UINT64:
        return CAST_TO_UINT64_FUNC_NAME;
    case LogicalTypeID::UINT32:
        return CAST_TO_UINT32_FUNC_NAME;
    case LogicalTypeID::UINT16:
        return CAST_TO_UINT16_FUNC_NAME;
    case LogicalTypeID::UINT8:
        return CAST_TO_UINT8_FUNC_NAME;
    case LogicalTypeID::INT128:
        return CAST_TO_INT128_FUNC_NAME;
    case LogicalTypeID::FLOAT:
        return CAST_TO_FLOAT_FUNC_NAME;
    case LogicalTypeID::DOUBLE:
        return CAST_TO_DOUBLE_FUNC_NAME;
    case LogicalTypeID::DATE:
        return CAST_TO_DATE_FUNC_NAME;
    case LogicalTypeID::TIMESTAMP:
        return CAST_TO_TIMESTAMP_FUNC_NAME;
    case LogicalTypeID::INTERVAL:
        return CAST_TO_INTERVAL_FUNC_NAME;
    case LogicalTypeID::STRING:
        return CAST_TO_STRING_FUNC_NAME;
    default:
        // LCOV_EXCL_START
        throw NotImplementedException("bindImplicitCastFuncName()");
        // LCOV_EXCL_END
    }
}

void CastFunction::bindImplicitCastFunc(
    LogicalTypeID sourceTypeID, LogicalTypeID targetTypeID, scalar_exec_func& func) {
    switch (targetTypeID) {
    case LogicalTypeID::INT8: {
        bindImplicitNumericalCastFunc<int8_t, CastToInt8>(sourceTypeID, func);
        return;
    }
    case LogicalTypeID::INT16: {
        bindImplicitNumericalCastFunc<int16_t, CastToInt16>(sourceTypeID, func);
        return;
    }
    case LogicalTypeID::INT32: {
        bindImplicitNumericalCastFunc<int32_t, CastToInt32>(sourceTypeID, func);
        return;
    }
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        bindImplicitNumericalCastFunc<int64_t, CastToInt64>(sourceTypeID, func);
        return;
    }
    case LogicalTypeID::UINT8: {
        bindImplicitNumericalCastFunc<uint8_t, CastToUInt8>(sourceTypeID, func);
        return;
    }
    case LogicalTypeID::UINT16: {
        bindImplicitNumericalCastFunc<uint16_t, CastToUInt16>(sourceTypeID, func);
        return;
    }
    case LogicalTypeID::UINT32: {
        bindImplicitNumericalCastFunc<uint32_t, CastToUInt32>(sourceTypeID, func);
        return;
    }
    case LogicalTypeID::UINT64: {
        bindImplicitNumericalCastFunc<uint64_t, CastToUInt64>(sourceTypeID, func);
        return;
    }
    case LogicalTypeID::INT128: {
        bindImplicitNumericalCastFunc<int128_t, CastToInt128>(sourceTypeID, func);
        return;
    }
    case LogicalTypeID::FLOAT: {
        bindImplicitNumericalCastFunc<float_t, CastToFloat>(sourceTypeID, func);
        return;
    }
    case LogicalTypeID::DOUBLE: {
        bindImplicitNumericalCastFunc<double_t, CastToDouble>(sourceTypeID, func);
        return;
    }
    case LogicalTypeID::DATE: {
        assert(sourceTypeID == LogicalTypeID::STRING);
        func = &ScalarFunction::UnaryStringExecFunction<ku_string_t, date_t, CastStringToTypes>;
        return;
    }
    case LogicalTypeID::TIMESTAMP: {
        assert(sourceTypeID == LogicalTypeID::STRING || sourceTypeID == LogicalTypeID::DATE);
        func = sourceTypeID == LogicalTypeID::STRING ?
                   &ScalarFunction::UnaryStringExecFunction<ku_string_t, timestamp_t,
                       CastStringToTypes> :
                   &ScalarFunction::UnaryExecFunction<date_t, timestamp_t, CastDateToTimestamp>;
        return;
    }
    case LogicalTypeID::INTERVAL: {
        assert(sourceTypeID == LogicalTypeID::STRING);
        func = &ScalarFunction::UnaryStringExecFunction<ku_string_t, interval_t, CastStringToTypes>;
        return;
    }
    default:
        // LCOV_EXCL_START
        throw NotImplementedException("Unimplemented casting operation from " +
                                      LogicalTypeUtils::dataTypeToString(sourceTypeID) + " to " +
                                      LogicalTypeUtils::dataTypeToString(targetTypeID) + ".");
        // LCOV_EXCL_END
    }
}

function_set CastToDateFunction::getFunctionSet() {
    function_set result;
    result.push_back(CastFunction::CastFunction::bindCastStringToFunction<date_t>(
        CAST_TO_DATE_FUNC_NAME, LogicalTypeID::DATE));
    return result;
}

function_set CastToTimestampFunction::getFunctionSet() {
    function_set result;
    result.push_back(CastFunction::CastFunction::bindCastStringToFunction<timestamp_t>(
        CAST_TO_TIMESTAMP_FUNC_NAME, LogicalTypeID::TIMESTAMP));
    return result;
}

function_set CastToIntervalFunction::getFunctionSet() {
    function_set result;
    result.push_back(CastFunction::CastFunction::bindCastStringToFunction<interval_t>(
        CAST_TO_INTERVAL_FUNC_NAME, LogicalTypeID::INTERVAL));
    return result;
}

void castFixedListToString(
    ValueVector& param, uint64_t pos, ValueVector& resultVector, uint64_t resultPos) {
    resultVector.setNull(resultPos, param.isNull(pos));
    if (param.isNull(pos)) {
        return;
    }
    std::string result = "[";
    auto numValuesPerList = FixedListType::getNumElementsInList(&param.dataType);
    auto childType = FixedListType::getChildType(&param.dataType);
    auto values = param.getData() + pos * param.getNumBytesPerValue();
    for (auto i = 0u; i < numValuesPerList - 1; ++i) {
        // Note: FixedList can only store numeric types and doesn't allow nulls.
        result += TypeUtils::castValueToString(*childType, values, nullptr /* vector */);
        result += ",";
        values += PhysicalTypeUtils::getFixedTypeSize(childType->getPhysicalType());
    }
    result += TypeUtils::castValueToString(*childType, values, nullptr /* vector */);
    result += "]";
    resultVector.setValue(resultPos, result);
}

void fixedListCastExecFunction(
    const std::vector<std::shared_ptr<ValueVector>>& params, common::ValueVector& result) {
    assert(params.size() == 1);
    auto param = params[0];
    if (param->state->isFlat()) {
        castFixedListToString(*param, param->state->selVector->selectedPositions[0], result,
            result.state->selVector->selectedPositions[0]);
    } else if (param->state->selVector->isUnfiltered()) {
        for (auto i = 0u; i < param->state->selVector->selectedSize; i++) {
            castFixedListToString(*param, i, result, i);
        }
    } else {
        for (auto i = 0u; i < param->state->selVector->selectedSize; i++) {
            castFixedListToString(*param, param->state->selVector->selectedPositions[i], result,
                result.state->selVector->selectedPositions[i]);
        }
    }
}

void CastToStringFunction::getUnaryCastToStringExecFunction(
    LogicalTypeID typeID, scalar_exec_func& func) {
    switch (typeID) {
    case common::LogicalTypeID::BOOL: {
        func = ScalarFunction::UnaryCastExecFunction<bool, ku_string_t, CastToString>;
    } break;
    case common::LogicalTypeID::SERIAL:
    case common::LogicalTypeID::INT64: {
        func = ScalarFunction::UnaryCastExecFunction<int64_t, ku_string_t, CastToString>;
    } break;
    case common::LogicalTypeID::INT32: {
        func = ScalarFunction::UnaryCastExecFunction<int32_t, ku_string_t, CastToString>;
    } break;
    case common::LogicalTypeID::INT16: {
        func = ScalarFunction::UnaryCastExecFunction<int16_t, ku_string_t, CastToString>;
    } break;
    case common::LogicalTypeID::INT8: {
        func = ScalarFunction::UnaryCastExecFunction<int8_t, ku_string_t, CastToString>;
    } break;
    case common::LogicalTypeID::UINT64: {
        func = ScalarFunction::UnaryCastExecFunction<uint64_t, ku_string_t, CastToString>;
    } break;
    case common::LogicalTypeID::UINT32: {
        func = ScalarFunction::UnaryCastExecFunction<uint32_t, ku_string_t, CastToString>;
    } break;
    case common::LogicalTypeID::UINT16: {
        func = ScalarFunction::UnaryCastExecFunction<uint16_t, ku_string_t, CastToString>;
    } break;
    case common::LogicalTypeID::INT128: {
        func = ScalarFunction::UnaryCastExecFunction<int128_t, ku_string_t, CastToString>;
    } break;
    case common::LogicalTypeID::UINT8: {
        func = ScalarFunction::UnaryCastExecFunction<uint8_t, ku_string_t, CastToString>;
    } break;
    case common::LogicalTypeID::DOUBLE: {
        func = ScalarFunction::UnaryCastExecFunction<double_t, ku_string_t, CastToString>;
    } break;
    case common::LogicalTypeID::FLOAT: {
        func = ScalarFunction::UnaryCastExecFunction<float_t, ku_string_t, CastToString>;
    } break;
    case common::LogicalTypeID::DATE: {
        func = ScalarFunction::UnaryCastExecFunction<date_t, ku_string_t, CastToString>;
    } break;
    case common::LogicalTypeID::TIMESTAMP: {
        func = ScalarFunction::UnaryCastExecFunction<timestamp_t, ku_string_t, CastToString>;
    } break;
    case common::LogicalTypeID::INTERVAL: {
        func = ScalarFunction::UnaryCastExecFunction<interval_t, ku_string_t, CastToString>;
    } break;
    case common::LogicalTypeID::INTERNAL_ID: {
        func = ScalarFunction::UnaryCastExecFunction<internalID_t, ku_string_t, CastToString>;
    } break;
    case common::LogicalTypeID::BLOB: {
        func = ScalarFunction::UnaryCastExecFunction<blob_t, ku_string_t, CastToString>;
    } break;
    case common::LogicalTypeID::STRING: {
        func = ScalarFunction::UnaryCastExecFunction<ku_string_t, ku_string_t, CastToString>;
    } break;
    case common::LogicalTypeID::VAR_LIST: {
        func = ScalarFunction::UnaryCastExecFunction<list_entry_t, ku_string_t, CastToString>;
    } break;
    case common::LogicalTypeID::FIXED_LIST: {
        func = fixedListCastExecFunction;
    } break;
    case common::LogicalTypeID::MAP: {
        func = ScalarFunction::UnaryCastExecFunction<map_entry_t, ku_string_t, CastToString>;
    } break;
    case common::LogicalTypeID::NODE:
    case common::LogicalTypeID::REL:
    case common::LogicalTypeID::STRUCT: {
        func = ScalarFunction::UnaryCastExecFunction<struct_entry_t, ku_string_t, CastToString>;
    } break;
    case common::LogicalTypeID::UNION: {
        func = ScalarFunction::UnaryCastExecFunction<union_entry_t, ku_string_t, CastToString>;
    } break;
    default:
        // LCOV_EXCL_START
        throw common::NotImplementedException{
            "CastToStringFunction::getUnaryCastToStringExecFunction"};
        // LCOV_EXCL_END
    }
}

function_set CastToStringFunction::getFunctionSet() {
    function_set result;
    result.reserve(LogicalTypeUtils::getAllValidLogicTypes().size());
    for (auto& type : LogicalTypeUtils::getAllValidLogicTypes()) {
        scalar_exec_func execFunc;
        getUnaryCastToStringExecFunction(type.getLogicalTypeID(), execFunc);
        auto definition = std::make_unique<ScalarFunction>(CAST_TO_STRING_FUNC_NAME,
            std::vector<LogicalTypeID>{type.getLogicalTypeID()}, LogicalTypeID::STRING, execFunc);
        result.push_back(std::move(definition));
    }
    return result;
}

function_set CastToBlobFunction::getFunctionSet() {
    function_set result;
    result.push_back(CastFunction::CastFunction::bindCastStringToFunction<blob_t>(
        CAST_TO_BLOB_FUNC_NAME, LogicalTypeID::BLOB));
    return result;
}

function_set CastToBoolFunction::getFunctionSet() {
    function_set result;
    result.push_back(CastFunction::CastFunction::bindCastStringToFunction<bool>(
        CAST_TO_BOOL_FUNC_NAME, LogicalTypeID::BOOL));
    return result;
}

function_set CastToDoubleFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(CastFunction::bindNumericCastFunction<double_t, CastToDouble>(
            CAST_TO_DOUBLE_FUNC_NAME, typeID, LogicalTypeID::DOUBLE));
    }
    result.push_back(CastFunction::bindCastStringToFunction<double_t>(
        CAST_TO_DOUBLE_FUNC_NAME, LogicalTypeID::DOUBLE));
    return result;
}

function_set CastToFloatFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(CastFunction::bindNumericCastFunction<float_t, CastToFloat>(
            CAST_TO_FLOAT_FUNC_NAME, typeID, LogicalTypeID::FLOAT));
    }
    result.push_back(CastFunction::bindCastStringToFunction<float_t>(
        CAST_TO_FLOAT_FUNC_NAME, LogicalTypeID::FLOAT));
    return result;
}

function_set CastToInt128Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(CastFunction::bindNumericCastFunction<int128_t, CastToInt128>(
            CAST_TO_INT128_FUNC_NAME, typeID, LogicalTypeID::INT128));
    }
    result.push_back(CastFunction::bindCastStringToFunction<int128_t>(
        CAST_TO_INT128_FUNC_NAME, LogicalTypeID::INT128));
    return result;
}

function_set CastToSerialFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(CastFunction::bindNumericCastFunction<int64_t, CastToSerial>(
            CAST_TO_SERIAL_FUNC_NAME, typeID, LogicalTypeID::SERIAL));
    }
    result.push_back(CastFunction::bindCastStringToFunction<int64_t>(
        CAST_TO_SERIAL_FUNC_NAME, LogicalTypeID::INT64));
    return result;
}

function_set CastToInt64Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(CastFunction::bindNumericCastFunction<int64_t, CastToInt64>(
            CAST_TO_INT64_FUNC_NAME, typeID, LogicalTypeID::INT64));
    }
    result.push_back(CastFunction::bindCastStringToFunction<int64_t>(
        CAST_TO_INT64_FUNC_NAME, LogicalTypeID::INT64));
    return result;
}

function_set CastToInt32Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(CastFunction::bindNumericCastFunction<int32_t, CastToInt32>(
            CAST_TO_INT32_FUNC_NAME, typeID, LogicalTypeID::INT32));
    }
    result.push_back(CastFunction::bindCastStringToFunction<int32_t>(
        CAST_TO_INT32_FUNC_NAME, LogicalTypeID::INT32));
    return result;
}

function_set CastToInt16Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(CastFunction::bindNumericCastFunction<int16_t, CastToInt16>(
            CAST_TO_INT16_FUNC_NAME, typeID, LogicalTypeID::INT16));
    }
    result.push_back(CastFunction::bindCastStringToFunction<int16_t>(
        CAST_TO_INT16_FUNC_NAME, LogicalTypeID::INT16));
    return result;
}

function_set CastToInt8Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(CastFunction::bindNumericCastFunction<int8_t, CastToInt8>(
            CAST_TO_INT8_FUNC_NAME, typeID, LogicalTypeID::INT8));
    }
    result.push_back(CastFunction::bindCastStringToFunction<int8_t>(
        CAST_TO_INT8_FUNC_NAME, LogicalTypeID::INT8));
    return result;
}

function_set CastToUInt64Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(CastFunction::bindNumericCastFunction<uint64_t, CastToUInt64>(
            CAST_TO_UINT64_FUNC_NAME, typeID, LogicalTypeID::UINT64));
    }
    result.push_back(CastFunction::bindCastStringToFunction<uint64_t>(
        CAST_TO_UINT64_FUNC_NAME, LogicalTypeID::UINT64));
    return result;
}

function_set CastToUInt32Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(CastFunction::bindNumericCastFunction<uint32_t, CastToUInt32>(
            CAST_TO_UINT32_FUNC_NAME, typeID, LogicalTypeID::UINT32));
    }
    result.push_back(CastFunction::bindCastStringToFunction<uint32_t>(
        CAST_TO_UINT32_FUNC_NAME, LogicalTypeID::UINT32));
    return result;
}

function_set CastToUInt16Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(CastFunction::bindNumericCastFunction<uint16_t, CastToUInt16>(
            CAST_TO_UINT16_FUNC_NAME, typeID, LogicalTypeID::UINT16));
    }
    result.push_back(CastFunction::bindCastStringToFunction<uint16_t>(
        CAST_TO_UINT16_FUNC_NAME, LogicalTypeID::UINT16));
    return result;
}

function_set CastToUInt8Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(CastFunction::bindNumericCastFunction<uint8_t, CastToUInt8>(
            CAST_TO_UINT8_FUNC_NAME, typeID, LogicalTypeID::UINT8));
    }
    result.push_back(CastFunction::bindCastStringToFunction<uint8_t>(
        CAST_TO_UINT8_FUNC_NAME, LogicalTypeID::UINT8));
    return result;
}

} // namespace function
} // namespace kuzu
