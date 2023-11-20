#include "function/cast/vector_cast_functions.h"

#include "binder/binder.h"
#include "binder/expression/literal_expression.h"
#include "common/exception/binder.h"
#include "common/exception/conversion.h"
#include "function/cast/functions/cast_fixed_list.h"
#include "function/cast/functions/cast_from_string_functions.h"
#include "function/cast/functions/cast_functions.h"
#include "function/cast/functions/cast_rdf_variant.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

template<typename /*EXECUTOR*/ = UnaryFunctionExecutor>
static void fixedListToListCastExecFunction(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr) {
    KU_ASSERT(params.size() == 1);
    auto inputVector = params[0];

    auto numValuesPerList = FixedListType::getNumValuesInList(&inputVector->dataType);
    for (auto i = 0u; i < inputVector->state->selVector->selectedSize; i++) {
        auto pos = inputVector->state->selVector->selectedPositions[i];
        result.setNull(pos, inputVector->isNull(pos));
        if (!result.isNull(pos)) {
            list_entry_t listEntry{pos * numValuesPerList, numValuesPerList};
            result.setValue(pos, listEntry);
        }
    }
    auto numOfEntries = inputVector->state->selVector
                            ->selectedPositions[inputVector->state->selVector->selectedSize - 1] +
                        1;
    ListVector::resizeDataVector(&result, numOfEntries * numValuesPerList);

    auto resultVector = ListVector::getDataVector(&result);
    scalar_exec_func func = CastFunction::bindCastFunction<CastFixedListToListFunctionExecutor>(
        "CAST", FixedListType::getChildType(&inputVector->dataType)->getLogicalTypeID(),
        resultVector->dataType.getLogicalTypeID())
                                ->execFunc;
    reinterpret_cast<CastFunctionBindData*>(dataPtr)->numOfEntries = numOfEntries;
    func(params, *resultVector, dataPtr);
}

template<>
void fixedListToListCastExecFunction<CastChildFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr) {
    KU_ASSERT(params.size() == 1);
    auto inputVector = params[0];

    auto numOfEntries = reinterpret_cast<CastFunctionBindData*>(dataPtr)->numOfEntries;
    result.setNullFromBits(inputVector->getNullMaskData(), 0, 0, numOfEntries);
    auto numValuesPerList = FixedListType::getNumValuesInList(&inputVector->dataType);
    ListVector::resizeDataVector(&result, numOfEntries * numValuesPerList);

    for (auto i = 0u; i < numOfEntries; i++) {
        result.setNull(i, inputVector->isNull(i));
        if (!result.isNull(i)) {
            list_entry_t listEntry{i * numValuesPerList, numValuesPerList};
            result.setValue(i, listEntry);
        }
    }

    auto resultVector = ListVector::getDataVector(&result);
    scalar_exec_func func = CastFunction::bindCastFunction<CastFixedListToListFunctionExecutor>(
        "CAST", FixedListType::getChildType(&inputVector->dataType)->getLogicalTypeID(),
        resultVector->dataType.getLogicalTypeID())
                                ->execFunc;
    func(params, *resultVector, dataPtr);
}

static void resolveNestedVector(std::shared_ptr<ValueVector> inputVector, ValueVector* resultVector,
    uint64_t numOfEntries, CastFunctionBindData* dataPtr) {
    auto inputType = &inputVector->dataType;
    auto resultType = &resultVector->dataType;
    while (true) {
        if (inputType->getPhysicalType() == PhysicalTypeID::VAR_LIST &&
            resultType->getPhysicalType() == PhysicalTypeID::VAR_LIST) {
            // copy data and nullmask from input
            memcpy(resultVector->getData(), inputVector->getData(),
                numOfEntries * resultVector->getNumBytesPerValue());
            resultVector->setNullFromBits(inputVector->getNullMaskData(), 0, 0, numOfEntries);

            numOfEntries = ListVector::getDataVectorSize(inputVector.get());
            ListVector::resizeDataVector(resultVector, numOfEntries);

            inputVector = ListVector::getSharedDataVector(inputVector.get());
            resultVector = ListVector::getDataVector(resultVector);
            inputType = &inputVector->dataType;
            resultType = &resultVector->dataType;
        } else if (inputType->getLogicalTypeID() == LogicalTypeID::STRUCT &&
                   resultType->getLogicalTypeID() == LogicalTypeID::STRUCT) {
            // check if struct type can be cast
            auto errorMsg = stringFormat("Unsupported casting function from {} to {}.",
                inputType->toString(), resultType->toString());
            auto inputTypeNames = StructType::getFieldNames(inputType);
            auto resultTypeNames = StructType::getFieldNames(resultType);
            if (inputTypeNames.size() != resultTypeNames.size()) {
                throw ConversionException{errorMsg};
            }
            for (auto i = 0u; i < inputTypeNames.size(); i++) {
                if (inputTypeNames[i] != resultTypeNames[i]) {
                    throw ConversionException{errorMsg};
                }
            }

            // copy data and nullmask from input
            memcpy(resultVector->getData(), inputVector->getData(),
                numOfEntries * resultVector->getNumBytesPerValue());
            resultVector->setNullFromBits(inputVector->getNullMaskData(), 0, 0, numOfEntries);

            auto inputFieldVectors = StructVector::getFieldVectors(inputVector.get());
            auto resultFieldVectors = StructVector::getFieldVectors(resultVector);
            for (auto i = 0u; i < inputFieldVectors.size(); i++) {
                resolveNestedVector(
                    inputFieldVectors[i], resultFieldVectors[i].get(), numOfEntries, dataPtr);
            }
            return;
        } else {
            break;
        }
    }

    // non-nested types
    scalar_exec_func func = CastFunction::bindCastFunction<CastChildFunctionExecutor>(
        "CAST", inputType->getLogicalTypeID(), resultType->getLogicalTypeID())
                                ->execFunc;
    std::vector<std::shared_ptr<ValueVector>> childParams{inputVector};
    dataPtr->numOfEntries = numOfEntries;
    func(childParams, *resultVector, (void*)dataPtr);
}

static void nestedTypesCastExecFunction(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr) {
    KU_ASSERT(params.size() == 1);
    result.resetAuxiliaryBuffer();
    auto inputVector = params[0];

    // check if all selcted list entry have the requried fixed list size
    if (CastFixedListHelper::containsListToFixedList(&inputVector->dataType, &result.dataType)) {
        for (auto i = 0u; i < inputVector->state->selVector->selectedSize; i++) {
            auto pos = inputVector->state->selVector->selectedPositions[i];
            CastFixedListHelper::validateListEntry(inputVector.get(), &result.dataType, pos);
        }
    };

    auto numOfEntries = inputVector->state->selVector
                            ->selectedPositions[inputVector->state->selVector->selectedSize - 1] +
                        1;
    resolveNestedVector(
        inputVector, &result, numOfEntries, reinterpret_cast<CastFunctionBindData*>(dataPtr));
}

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
    case LogicalTypeID::RDF_VARIANT: {
        // Implicit cast from
        return true;
    }
    default:
        return false;
    }
}

template<typename EXECUTOR = UnaryFunctionExecutor>
static std::unique_ptr<ScalarFunction> bindCastFromStringFunction(
    const std::string& functionName, LogicalTypeID targetTypeID) {
    scalar_exec_func execFunc;
    switch (targetTypeID) {
    case LogicalTypeID::DATE: {
        execFunc =
            ScalarFunction::UnaryCastStringExecFunction<ku_string_t, date_t, CastString, EXECUTOR>;
    } break;
    case LogicalTypeID::TIMESTAMP: {
        execFunc = ScalarFunction::UnaryCastStringExecFunction<ku_string_t, timestamp_t, CastString,
            EXECUTOR>;
    } break;
    case LogicalTypeID::INTERVAL: {
        execFunc = ScalarFunction::UnaryCastStringExecFunction<ku_string_t, interval_t, CastString,
            EXECUTOR>;
    } break;
    case LogicalTypeID::BLOB: {
        execFunc =
            ScalarFunction::UnaryCastStringExecFunction<ku_string_t, blob_t, CastString, EXECUTOR>;
    } break;
    case LogicalTypeID::STRING: {
        execFunc =
            ScalarFunction::UnaryCastExecFunction<ku_string_t, ku_string_t, CastToString, EXECUTOR>;
    } break;
    case LogicalTypeID::BOOL: {
        execFunc =
            ScalarFunction::UnaryCastStringExecFunction<ku_string_t, bool, CastString, EXECUTOR>;
    } break;
    case LogicalTypeID::DOUBLE: {
        execFunc = ScalarFunction::UnaryCastStringExecFunction<ku_string_t, double_t, CastString,
            EXECUTOR>;
    } break;
    case LogicalTypeID::FLOAT: {
        execFunc =
            ScalarFunction::UnaryCastStringExecFunction<ku_string_t, float, CastString, EXECUTOR>;
    } break;
    case LogicalTypeID::INT128: {
        execFunc = ScalarFunction::UnaryCastStringExecFunction<ku_string_t, int128_t, CastString,
            EXECUTOR>;
    } break;
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        execFunc =
            ScalarFunction::UnaryCastStringExecFunction<ku_string_t, int64_t, CastString, EXECUTOR>;
    } break;
    case LogicalTypeID::INT32: {
        execFunc =
            ScalarFunction::UnaryCastStringExecFunction<ku_string_t, int32_t, CastString, EXECUTOR>;
    } break;
    case LogicalTypeID::INT16: {
        execFunc =
            ScalarFunction::UnaryCastStringExecFunction<ku_string_t, int16_t, CastString, EXECUTOR>;
    } break;
    case LogicalTypeID::INT8: {
        execFunc =
            ScalarFunction::UnaryCastStringExecFunction<ku_string_t, int8_t, CastString, EXECUTOR>;
    } break;
    case LogicalTypeID::UINT64: {
        execFunc = ScalarFunction::UnaryCastStringExecFunction<ku_string_t, uint64_t, CastString,
            EXECUTOR>;
    } break;
    case LogicalTypeID::UINT32: {
        execFunc = ScalarFunction::UnaryCastStringExecFunction<ku_string_t, uint32_t, CastString,
            EXECUTOR>;
    } break;
    case LogicalTypeID::UINT16: {
        execFunc = ScalarFunction::UnaryCastStringExecFunction<ku_string_t, uint16_t, CastString,
            EXECUTOR>;
    } break;
    case LogicalTypeID::UINT8: {
        execFunc =
            ScalarFunction::UnaryCastStringExecFunction<ku_string_t, uint8_t, CastString, EXECUTOR>;
    } break;
    case LogicalTypeID::VAR_LIST: {
        execFunc = ScalarFunction::UnaryCastStringExecFunction<ku_string_t, list_entry_t,
            CastString, EXECUTOR>;
    } break;
    case LogicalTypeID::FIXED_LIST: {
        execFunc = CastFixedList::stringtoFixedListCastExecFunction<EXECUTOR>;
    } break;
    case LogicalTypeID::MAP: {
        execFunc = ScalarFunction::UnaryCastStringExecFunction<ku_string_t, map_entry_t, CastString,
            EXECUTOR>;
    } break;
    case LogicalTypeID::STRUCT: {
        execFunc = ScalarFunction::UnaryCastStringExecFunction<ku_string_t, struct_entry_t,
            CastString, EXECUTOR>;
    } break;
    case LogicalTypeID::UNION: {
        execFunc = ScalarFunction::UnaryCastStringExecFunction<ku_string_t, union_entry_t,
            CastString, EXECUTOR>;
    } break;
    default:
        throw ConversionException{stringFormat("Unsupported casting function from STRING to {}.",
            LogicalTypeUtils::toString(targetTypeID))};
    }
    return std::make_unique<ScalarFunction>(
        functionName, std::vector<LogicalTypeID>{LogicalTypeID::STRING}, targetTypeID, execFunc);
}

template<typename EXECUTOR = UnaryFunctionExecutor>
static std::unique_ptr<ScalarFunction> bindCastFromRdfVariantFunction(
    const std::string& functionName, LogicalTypeID targetTypeID) {
    scalar_exec_func execFunc;
    switch (targetTypeID) {
    case LogicalTypeID::DATE: {
        execFunc = ScalarFunction::UnaryTryCastExecFunction<struct_entry_t, date_t,
            CastFromRdfVariant, EXECUTOR>;
    } break;
    case LogicalTypeID::TIMESTAMP: {
        execFunc = ScalarFunction::UnaryTryCastExecFunction<struct_entry_t, timestamp_t,
            CastFromRdfVariant, EXECUTOR>;
    } break;
    case LogicalTypeID::INTERVAL: {
        execFunc = ScalarFunction::UnaryTryCastExecFunction<struct_entry_t, interval_t,
            CastFromRdfVariant, EXECUTOR>;
    } break;
    case LogicalTypeID::BLOB: {
        execFunc = ScalarFunction::UnaryTryCastExecFunction<struct_entry_t, blob_t,
            CastFromRdfVariant, EXECUTOR>;
    } break;
    case LogicalTypeID::STRING: {
        execFunc = ScalarFunction::UnaryTryCastExecFunction<struct_entry_t, ku_string_t,
            CastFromRdfVariant, EXECUTOR>;
    } break;
    case LogicalTypeID::BOOL: {
        execFunc = ScalarFunction::UnaryTryCastExecFunction<struct_entry_t, bool,
            CastFromRdfVariant, EXECUTOR>;
    } break;
    case LogicalTypeID::DOUBLE: {
        execFunc = ScalarFunction::UnaryTryCastExecFunction<struct_entry_t, double_t,
            CastFromRdfVariant, EXECUTOR>;
    } break;
    case LogicalTypeID::FLOAT: {
        execFunc = ScalarFunction::UnaryTryCastExecFunction<struct_entry_t, float_t,
            CastFromRdfVariant, EXECUTOR>;
    } break;
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        execFunc = ScalarFunction::UnaryTryCastExecFunction<struct_entry_t, int64_t,
            CastFromRdfVariant, EXECUTOR>;
    } break;
    case LogicalTypeID::INT32: {
        execFunc = ScalarFunction::UnaryTryCastExecFunction<struct_entry_t, int32_t,
            CastFromRdfVariant, EXECUTOR>;
    } break;
    case LogicalTypeID::INT16: {
        execFunc = ScalarFunction::UnaryTryCastExecFunction<struct_entry_t, int16_t,
            CastFromRdfVariant, EXECUTOR>;
    } break;
    case LogicalTypeID::INT8: {
        execFunc = ScalarFunction::UnaryTryCastExecFunction<struct_entry_t, int8_t,
            CastFromRdfVariant, EXECUTOR>;
    } break;
    case LogicalTypeID::UINT64: {
        execFunc = ScalarFunction::UnaryTryCastExecFunction<struct_entry_t, uint64_t,
            CastFromRdfVariant, EXECUTOR>;
    } break;
    case LogicalTypeID::UINT32: {
        execFunc = ScalarFunction::UnaryTryCastExecFunction<struct_entry_t, uint32_t,
            CastFromRdfVariant, EXECUTOR>;
    } break;
    case LogicalTypeID::UINT16: {
        execFunc = ScalarFunction::UnaryTryCastExecFunction<struct_entry_t, uint16_t,
            CastFromRdfVariant, EXECUTOR>;
    } break;
    case LogicalTypeID::UINT8: {
        execFunc = ScalarFunction::UnaryTryCastExecFunction<struct_entry_t, uint8_t,
            CastFromRdfVariant, EXECUTOR>;
    } break;
        // LCOV_EXCL_START
    default:
        throw ConversionException{
            stringFormat("Unsupported casting function from RDF_VARIANT to {}.",
                LogicalTypeUtils::toString(targetTypeID))};
        // LCOV_EXCL_STOP
    }
    return std::make_unique<ScalarFunction>(functionName,
        std::vector<LogicalTypeID>{LogicalTypeID::RDF_VARIANT}, targetTypeID, execFunc);
}

template<typename EXECUTOR = UnaryFunctionExecutor>
static std::unique_ptr<ScalarFunction> bindCastToStringFunction(
    const std::string& functionName, LogicalTypeID sourceTypeID) {
    scalar_exec_func func;
    switch (sourceTypeID) {
    case LogicalTypeID::BOOL: {
        func = ScalarFunction::UnaryCastExecFunction<bool, ku_string_t, CastToString, EXECUTOR>;
    } break;
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        func = ScalarFunction::UnaryCastExecFunction<int64_t, ku_string_t, CastToString, EXECUTOR>;
    } break;
    case LogicalTypeID::INT32: {
        func = ScalarFunction::UnaryCastExecFunction<int32_t, ku_string_t, CastToString, EXECUTOR>;
    } break;
    case LogicalTypeID::INT16: {
        func = ScalarFunction::UnaryCastExecFunction<int16_t, ku_string_t, CastToString, EXECUTOR>;
    } break;
    case LogicalTypeID::INT8: {
        func = ScalarFunction::UnaryCastExecFunction<int8_t, ku_string_t, CastToString, EXECUTOR>;
    } break;
    case LogicalTypeID::UINT64: {
        func = ScalarFunction::UnaryCastExecFunction<uint64_t, ku_string_t, CastToString, EXECUTOR>;
    } break;
    case LogicalTypeID::UINT32: {
        func = ScalarFunction::UnaryCastExecFunction<uint32_t, ku_string_t, CastToString, EXECUTOR>;
    } break;
    case LogicalTypeID::UINT16: {
        func = ScalarFunction::UnaryCastExecFunction<uint16_t, ku_string_t, CastToString, EXECUTOR>;
    } break;
    case LogicalTypeID::INT128: {
        func = ScalarFunction::UnaryCastExecFunction<int128_t, ku_string_t, CastToString, EXECUTOR>;
    } break;
    case LogicalTypeID::UINT8: {
        func = ScalarFunction::UnaryCastExecFunction<uint8_t, ku_string_t, CastToString, EXECUTOR>;
    } break;
    case LogicalTypeID::DOUBLE: {
        func = ScalarFunction::UnaryCastExecFunction<double_t, ku_string_t, CastToString, EXECUTOR>;
    } break;
    case LogicalTypeID::FLOAT: {
        func = ScalarFunction::UnaryCastExecFunction<float_t, ku_string_t, CastToString, EXECUTOR>;
    } break;
    case LogicalTypeID::DATE: {
        func = ScalarFunction::UnaryCastExecFunction<date_t, ku_string_t, CastToString, EXECUTOR>;
    } break;
    case LogicalTypeID::TIMESTAMP: {
        func =
            ScalarFunction::UnaryCastExecFunction<timestamp_t, ku_string_t, CastToString, EXECUTOR>;
    } break;
    case LogicalTypeID::INTERVAL: {
        func =
            ScalarFunction::UnaryCastExecFunction<interval_t, ku_string_t, CastToString, EXECUTOR>;
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        func = ScalarFunction::UnaryCastExecFunction<internalID_t, ku_string_t, CastToString,
            EXECUTOR>;
    } break;
    case LogicalTypeID::BLOB: {
        func = ScalarFunction::UnaryCastExecFunction<blob_t, ku_string_t, CastToString, EXECUTOR>;
    } break;
    case LogicalTypeID::VAR_LIST: {
        func = ScalarFunction::UnaryCastExecFunction<list_entry_t, ku_string_t, CastToString,
            EXECUTOR>;
    } break;
    case LogicalTypeID::FIXED_LIST: {
        func = CastFixedList::fixedListToStringCastExecFunction<EXECUTOR>;
    } break;
    case LogicalTypeID::MAP: {
        func =
            ScalarFunction::UnaryCastExecFunction<map_entry_t, ku_string_t, CastToString, EXECUTOR>;
    } break;
    case LogicalTypeID::NODE:
    case LogicalTypeID::REL:
    case LogicalTypeID::STRUCT: {
        func = ScalarFunction::UnaryCastExecFunction<struct_entry_t, ku_string_t, CastToString,
            EXECUTOR>;
    } break;
    case LogicalTypeID::UNION: {
        func = ScalarFunction::UnaryCastExecFunction<union_entry_t, ku_string_t, CastToString,
            EXECUTOR>;
    } break;
        // ToDo(Kebing): RECURSIVE_REL to string
    default:
        KU_UNREACHABLE;
    }
    return std::make_unique<ScalarFunction>(
        functionName, std::vector<LogicalTypeID>{sourceTypeID}, LogicalTypeID::STRING, func);
}

template<typename DST_TYPE, typename OP, typename EXECUTOR = UnaryFunctionExecutor>
static std::unique_ptr<ScalarFunction> bindCastToNumericFunction(
    const std::string& functionName, LogicalTypeID sourceTypeID, LogicalTypeID targetTypeID) {
    scalar_exec_func func;
    switch (sourceTypeID) {
    case LogicalTypeID::INT8: {
        func = ScalarFunction::UnaryExecFunction<int8_t, DST_TYPE, OP, EXECUTOR>;
    } break;
    case LogicalTypeID::INT16: {
        func = ScalarFunction::UnaryExecFunction<int16_t, DST_TYPE, OP, EXECUTOR>;
    } break;
    case LogicalTypeID::INT32: {
        func = ScalarFunction::UnaryExecFunction<int32_t, DST_TYPE, OP, EXECUTOR>;
    } break;
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        func = ScalarFunction::UnaryExecFunction<int64_t, DST_TYPE, OP, EXECUTOR>;
    } break;
    case LogicalTypeID::UINT8: {
        func = ScalarFunction::UnaryExecFunction<uint8_t, DST_TYPE, OP, EXECUTOR>;
    } break;
    case LogicalTypeID::UINT16: {
        func = ScalarFunction::UnaryExecFunction<uint16_t, DST_TYPE, OP, EXECUTOR>;
    } break;
    case LogicalTypeID::UINT32: {
        func = ScalarFunction::UnaryExecFunction<uint32_t, DST_TYPE, OP, EXECUTOR>;
    } break;
    case LogicalTypeID::UINT64: {
        func = ScalarFunction::UnaryExecFunction<uint64_t, DST_TYPE, OP, EXECUTOR>;
    } break;
    case LogicalTypeID::INT128: {
        func = ScalarFunction::UnaryExecFunction<int128_t, DST_TYPE, OP, EXECUTOR>;
    } break;
    case LogicalTypeID::FLOAT: {
        func = ScalarFunction::UnaryExecFunction<float_t, DST_TYPE, OP, EXECUTOR>;
    } break;
    case LogicalTypeID::DOUBLE: {
        func = ScalarFunction::UnaryExecFunction<double_t, DST_TYPE, OP, EXECUTOR>;
    } break;
    default:
        throw ConversionException{stringFormat("Unsupported casting function from {} to {}.",
            LogicalTypeUtils::toString(sourceTypeID), LogicalTypeUtils::toString(targetTypeID))};
    }
    return std::make_unique<ScalarFunction>(
        functionName, std::vector<LogicalTypeID>{sourceTypeID}, targetTypeID, func);
}

template<typename EXECUTOR = UnaryFunctionExecutor>
static std::unique_ptr<ScalarFunction> bindCastBetweenNested(
    const std::string& functionName, LogicalTypeID sourceTypeID, LogicalTypeID targetTypeID) {
    switch (sourceTypeID) {
    case LogicalTypeID::VAR_LIST: {
        if (targetTypeID == LogicalTypeID::FIXED_LIST) {
            return std::make_unique<ScalarFunction>(functionName,
                std::vector<LogicalTypeID>{sourceTypeID}, targetTypeID,
                CastFixedList::listToFixedListCastExecFunction<EXECUTOR>);
        }
    }
    case LogicalTypeID::MAP:
    case LogicalTypeID::STRUCT: {
        if (sourceTypeID == targetTypeID) {
            return std::make_unique<ScalarFunction>(functionName,
                std::vector<LogicalTypeID>{sourceTypeID}, targetTypeID,
                nestedTypesCastExecFunction);
        }
    }
    case LogicalTypeID::FIXED_LIST: {
        if (targetTypeID == LogicalTypeID::VAR_LIST) {
            return std::make_unique<ScalarFunction>(functionName,
                std::vector<LogicalTypeID>{sourceTypeID}, targetTypeID,
                fixedListToListCastExecFunction<EXECUTOR>);
        } else if (sourceTypeID == targetTypeID) {
            return std::make_unique<ScalarFunction>(functionName,
                std::vector<LogicalTypeID>{sourceTypeID}, targetTypeID,
                CastFixedList::castBetweenFixedListExecFunc<EXECUTOR>);
        }
    }
    default:
        throw ConversionException{stringFormat("Unsupported casting function from {} to {}.",
            LogicalTypeUtils::toString(sourceTypeID), LogicalTypeUtils::toString(targetTypeID))};
    }
}

template<typename EXECUTOR = UnaryFunctionExecutor>
static std::unique_ptr<ScalarFunction> bindCastToTimestampFunction(
    const std::string& functionName, LogicalTypeID sourceTypeID) {
    scalar_exec_func func;
    switch (sourceTypeID) {
    case LogicalTypeID::DATE: {
        func =
            ScalarFunction::UnaryExecFunction<date_t, timestamp_t, CastDateToTimestamp, EXECUTOR>;
    } break;
    default:
        throw ConversionException{stringFormat("Unsupported casting function from {} to TIMESTAMP.",
            LogicalTypeUtils::toString(sourceTypeID))};
    }
    return std::make_unique<ScalarFunction>(
        functionName, std::vector<LogicalTypeID>{sourceTypeID}, LogicalTypeID::TIMESTAMP, func);
}

template<typename EXECUTOR>
std::unique_ptr<ScalarFunction> CastFunction::bindCastFunction(
    const std::string& functionName, LogicalTypeID sourceTypeID, LogicalTypeID targetTypeID) {
    if (sourceTypeID == LogicalTypeID::STRING) {
        return bindCastFromStringFunction<EXECUTOR>(functionName, targetTypeID);
    }
    if (sourceTypeID == LogicalTypeID::RDF_VARIANT) {
        return bindCastFromRdfVariantFunction<EXECUTOR>(functionName, targetTypeID);
    }
    switch (targetTypeID) {
    case LogicalTypeID::STRING: {
        return bindCastToStringFunction<EXECUTOR>(functionName, sourceTypeID);
    }
    case LogicalTypeID::DOUBLE: {
        return bindCastToNumericFunction<double_t, CastToDouble, EXECUTOR>(
            functionName, sourceTypeID, targetTypeID);
    }
    case LogicalTypeID::FLOAT: {
        return bindCastToNumericFunction<float_t, CastToFloat, EXECUTOR>(
            functionName, sourceTypeID, targetTypeID);
    }
    case LogicalTypeID::INT128: {
        return bindCastToNumericFunction<int128_t, CastToInt128, EXECUTOR>(
            functionName, sourceTypeID, targetTypeID);
    }
    case LogicalTypeID::SERIAL: {
        return bindCastToNumericFunction<int64_t, CastToSerial, EXECUTOR>(
            functionName, sourceTypeID, targetTypeID);
    }
    case LogicalTypeID::INT64: {
        return bindCastToNumericFunction<int64_t, CastToInt64, EXECUTOR>(
            functionName, sourceTypeID, targetTypeID);
    }
    case LogicalTypeID::INT32: {
        return bindCastToNumericFunction<int32_t, CastToInt32, EXECUTOR>(
            functionName, sourceTypeID, targetTypeID);
    }
    case LogicalTypeID::INT16: {
        return bindCastToNumericFunction<int16_t, CastToInt16, EXECUTOR>(
            functionName, sourceTypeID, targetTypeID);
    }
    case LogicalTypeID::INT8: {
        return bindCastToNumericFunction<int8_t, CastToInt8, EXECUTOR>(
            functionName, sourceTypeID, targetTypeID);
    }
    case LogicalTypeID::UINT64: {
        return bindCastToNumericFunction<uint64_t, CastToUInt64, EXECUTOR>(
            functionName, sourceTypeID, targetTypeID);
    }
    case LogicalTypeID::UINT32: {
        return bindCastToNumericFunction<uint32_t, CastToUInt32, EXECUTOR>(
            functionName, sourceTypeID, targetTypeID);
    }
    case LogicalTypeID::UINT16: {
        return bindCastToNumericFunction<uint16_t, CastToUInt16, EXECUTOR>(
            functionName, sourceTypeID, targetTypeID);
    }
    case LogicalTypeID::UINT8: {
        return bindCastToNumericFunction<uint8_t, CastToUInt8, EXECUTOR>(
            functionName, sourceTypeID, targetTypeID);
    }
    case LogicalTypeID::TIMESTAMP: {
        return bindCastToTimestampFunction<EXECUTOR>(functionName, sourceTypeID);
    }
    case LogicalTypeID::VAR_LIST:
    case LogicalTypeID::FIXED_LIST:
    case LogicalTypeID::MAP:
    case LogicalTypeID::STRUCT: {
        return bindCastBetweenNested<EXECUTOR>(functionName, sourceTypeID, targetTypeID);
    }
    default: {
        throw ConversionException{stringFormat("Unsupported casting function from {} to {}.",
            LogicalTypeUtils::toString(sourceTypeID), LogicalTypeUtils::toString(targetTypeID))};
    }
    }
}

function_set CastToDateFunction::getFunctionSet() {
    function_set result;
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_DATE_FUNC_NAME, LogicalTypeID::STRING, LogicalTypeID::DATE));
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_DATE_FUNC_NAME, LogicalTypeID::RDF_VARIANT, LogicalTypeID::DATE));
    return result;
}

function_set CastToTimestampFunction::getFunctionSet() {
    function_set result;
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_TIMESTAMP_FUNC_NAME, LogicalTypeID::STRING, LogicalTypeID::TIMESTAMP));
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_TIMESTAMP_FUNC_NAME, LogicalTypeID::RDF_VARIANT, LogicalTypeID::TIMESTAMP));
    return result;
}

function_set CastToIntervalFunction::getFunctionSet() {
    function_set result;
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_INTERVAL_FUNC_NAME, LogicalTypeID::STRING, LogicalTypeID::INTERVAL));
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_INTERVAL_FUNC_NAME, LogicalTypeID::RDF_VARIANT, LogicalTypeID::INTERVAL));
    return result;
}

function_set CastToStringFunction::getFunctionSet() {
    function_set result;
    result.reserve(LogicalTypeUtils::getAllValidLogicTypes().size());
    for (auto& type : LogicalTypeUtils::getAllValidLogicTypes()) {
        result.push_back(CastFunction::bindCastFunction(
            CAST_TO_STRING_FUNC_NAME, type.getLogicalTypeID(), LogicalTypeID::STRING));
    }
    return result;
}

function_set CastToBlobFunction::getFunctionSet() {
    function_set result;
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_BLOB_FUNC_NAME, LogicalTypeID::STRING, LogicalTypeID::BLOB));
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_BLOB_FUNC_NAME, LogicalTypeID::RDF_VARIANT, LogicalTypeID::BLOB));
    return result;
}

function_set CastToBoolFunction::getFunctionSet() {
    function_set result;
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_BOOL_FUNC_NAME, LogicalTypeID::STRING, LogicalTypeID::BOOL));
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_BOOL_FUNC_NAME, LogicalTypeID::RDF_VARIANT, LogicalTypeID::BOOL));
    return result;
}

function_set CastToDoubleFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(CastFunction::bindCastFunction(
            CAST_TO_DOUBLE_FUNC_NAME, typeID, LogicalTypeID::DOUBLE));
    }
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_DOUBLE_FUNC_NAME, LogicalTypeID::STRING, LogicalTypeID::DOUBLE));
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_DOUBLE_FUNC_NAME, LogicalTypeID::RDF_VARIANT, LogicalTypeID::DOUBLE));
    return result;
}

function_set CastToFloatFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(
            CastFunction::bindCastFunction(CAST_TO_FLOAT_FUNC_NAME, typeID, LogicalTypeID::FLOAT));
    }
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_FLOAT_FUNC_NAME, LogicalTypeID::STRING, LogicalTypeID::FLOAT));
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_FLOAT_FUNC_NAME, LogicalTypeID::RDF_VARIANT, LogicalTypeID::FLOAT));
    return result;
}

function_set CastToInt128Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(CastFunction::bindCastFunction(
            CAST_TO_INT128_FUNC_NAME, typeID, LogicalTypeID::INT128));
    }
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_INT128_FUNC_NAME, LogicalTypeID::STRING, LogicalTypeID::INT128));
    return result;
}

function_set CastToSerialFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(CastFunction::bindCastFunction(
            CAST_TO_SERIAL_FUNC_NAME, typeID, LogicalTypeID::SERIAL));
    }
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_SERIAL_FUNC_NAME, LogicalTypeID::STRING, LogicalTypeID::SERIAL));
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_SERIAL_FUNC_NAME, LogicalTypeID::RDF_VARIANT, LogicalTypeID::SERIAL));
    return result;
}

function_set CastToInt64Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(
            CastFunction::bindCastFunction(CAST_TO_INT64_FUNC_NAME, typeID, LogicalTypeID::INT64));
    }
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_INT64_FUNC_NAME, LogicalTypeID::STRING, LogicalTypeID::INT64));
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_INT64_FUNC_NAME, LogicalTypeID::RDF_VARIANT, LogicalTypeID::INT64));
    return result;
}

function_set CastToInt32Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(
            CastFunction::bindCastFunction(CAST_TO_INT32_FUNC_NAME, typeID, LogicalTypeID::INT32));
    }
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_INT32_FUNC_NAME, LogicalTypeID::STRING, LogicalTypeID::INT32));
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_INT32_FUNC_NAME, LogicalTypeID::RDF_VARIANT, LogicalTypeID::INT32));
    return result;
}

function_set CastToInt16Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(
            CastFunction::bindCastFunction(CAST_TO_INT16_FUNC_NAME, typeID, LogicalTypeID::INT16));
    }
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_INT16_FUNC_NAME, LogicalTypeID::STRING, LogicalTypeID::INT16));
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_INT16_FUNC_NAME, LogicalTypeID::RDF_VARIANT, LogicalTypeID::INT16));
    return result;
}

function_set CastToInt8Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(
            CastFunction::bindCastFunction(CAST_TO_INT8_FUNC_NAME, typeID, LogicalTypeID::INT8));
    }
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_INT8_FUNC_NAME, LogicalTypeID::STRING, LogicalTypeID::INT8));
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_INT8_FUNC_NAME, LogicalTypeID::RDF_VARIANT, LogicalTypeID::INT8));
    return result;
}

function_set CastToUInt64Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(CastFunction::bindCastFunction(
            CAST_TO_UINT64_FUNC_NAME, typeID, LogicalTypeID::UINT64));
    }
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_UINT64_FUNC_NAME, LogicalTypeID::STRING, LogicalTypeID::UINT64));
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_UINT64_FUNC_NAME, LogicalTypeID::RDF_VARIANT, LogicalTypeID::UINT64));
    return result;
}

function_set CastToUInt32Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(CastFunction::bindCastFunction(
            CAST_TO_UINT32_FUNC_NAME, typeID, LogicalTypeID::UINT32));
    }
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_UINT32_FUNC_NAME, LogicalTypeID::STRING, LogicalTypeID::UINT32));
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_UINT32_FUNC_NAME, LogicalTypeID::RDF_VARIANT, LogicalTypeID::UINT32));
    return result;
}

function_set CastToUInt16Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(CastFunction::bindCastFunction(
            CAST_TO_UINT16_FUNC_NAME, typeID, LogicalTypeID::UINT16));
    }
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_UINT16_FUNC_NAME, LogicalTypeID::STRING, LogicalTypeID::UINT16));
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_UINT16_FUNC_NAME, LogicalTypeID::RDF_VARIANT, LogicalTypeID::UINT16));
    return result;
}

function_set CastToUInt8Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(
            CastFunction::bindCastFunction(CAST_TO_UINT8_FUNC_NAME, typeID, LogicalTypeID::UINT8));
    }
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_UINT8_FUNC_NAME, LogicalTypeID::STRING, LogicalTypeID::UINT8));
    result.push_back(CastFunction::bindCastFunction(
        CAST_TO_UINT8_FUNC_NAME, LogicalTypeID::RDF_VARIANT, LogicalTypeID::UINT8));
    return result;
}

std::unique_ptr<FunctionBindData> CastAnyFunction::bindFunc(
    const binder::expression_vector& arguments, Function* function) {
    // check the size of the arguments
    if (arguments.size() != 2) {
        throw BinderException(stringFormat(
            "Invalid number of arguments for given function CAST. Expected: 2, Actual: {}.",
            arguments.size()));
    }

    auto inputTypeID = arguments[0]->dataType.getLogicalTypeID();
    if (inputTypeID == LogicalTypeID::ANY) {
        inputTypeID = LogicalTypeID::STRING;
    }
    auto str = ((binder::LiteralExpression&)*arguments[1]).getValue()->getValue<std::string>();
    auto outputType = binder::Binder::bindDataType(str);
    auto func = reinterpret_cast<ScalarFunction*>(function);
    func->name = "CAST_TO_" + str;
    func->parameterTypeIDs[0] = inputTypeID;
    func->execFunc =
        CastFunction::bindCastFunction(func->name, inputTypeID, outputType->getLogicalTypeID())
            ->execFunc;
    return std::make_unique<function::CastFunctionBindData>(*outputType);
}

function_set CastAnyFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(CAST_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY}, LogicalTypeID::ANY, nullptr, nullptr,
        bindFunc, false));
    return result;
}

} // namespace function
} // namespace kuzu
