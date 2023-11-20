#include "function/cast/vector_cast_functions.h"

#include "binder/binder.h"
#include "binder/expression/literal_expression.h"
#include "common/exception/binder.h"
#include "common/exception/conversion.h"
#include "common/exception/runtime.h"
#include "function/cast/functions/cast_from_string_functions.h"
#include "function/cast/functions/cast_functions.h"
#include "function/cast/functions/cast_rdf_variant.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static void castFixedListToString(
    ValueVector& param, uint64_t pos, ValueVector& resultVector, uint64_t resultPos) {
    resultVector.setNull(resultPos, param.isNull(pos));
    if (param.isNull(pos)) {
        return;
    }
    std::string result = "[";
    auto numValuesPerList = FixedListType::getNumValuesInList(&param.dataType);
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

template<typename /*EXECUTOR*/ = UnaryFunctionExecutor>
static void fixedListToStringCastExecFunction(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result,
    void* /*dataPtr*/) {
    KU_ASSERT(params.size() == 1);
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

template<>
void fixedListToStringCastExecFunction<CastChildFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr) {
    KU_ASSERT(params.size() == 1);

    auto inputVector = params[0].get();
    auto numOfEntries = reinterpret_cast<CastFunctionBindData*>(dataPtr)->numOfEntries;
    for (auto i = 0u; i < numOfEntries; i++) {
        castFixedListToString(*inputVector, i, result, i);
    }
}

template<typename /*EXECUTOR*/ = UnaryFunctionExecutor>
static void stringtoFixedListCastExecFunction(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr) {
    KU_ASSERT(params.size() == 1);
    auto param = params[0];
    auto csvReaderConfig = &reinterpret_cast<CastFunctionBindData*>(dataPtr)->csvConfig;
    if (param->state->isFlat()) {
        auto inputPos = param->state->selVector->selectedPositions[0];
        auto resultPos = result.state->selVector->selectedPositions[0];
        result.setNull(resultPos, param->isNull(inputPos));
        if (!result.isNull(inputPos)) {
            CastString::castToFixedList(
                param->getValue<ku_string_t>(inputPos), &result, resultPos, csvReaderConfig);
        }
    } else if (param->state->selVector->isUnfiltered()) {
        for (auto i = 0u; i < param->state->selVector->selectedSize; i++) {
            result.setNull(i, param->isNull(i));
            if (!result.isNull(i)) {
                CastString::castToFixedList(
                    param->getValue<ku_string_t>(i), &result, i, csvReaderConfig);
            }
        }
    } else {
        for (auto i = 0u; i < param->state->selVector->selectedSize; i++) {
            auto pos = param->state->selVector->selectedPositions[i];
            result.setNull(pos, param->isNull(pos));
            if (!result.isNull(pos)) {
                CastString::castToFixedList(
                    param->getValue<ku_string_t>(pos), &result, pos, csvReaderConfig);
            }
        }
    }
}

template<>
void stringtoFixedListCastExecFunction<CastChildFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr) {
    KU_ASSERT(params.size() == 1);
    auto numOfEntries = reinterpret_cast<CastFunctionBindData*>(dataPtr)->numOfEntries;
    auto csvReaderConfig = &reinterpret_cast<CastFunctionBindData*>(dataPtr)->csvConfig;

    auto inputVector = params[0].get();
    for (auto i = 0u; i < numOfEntries; i++) {
        result.setNull(i, inputVector->isNull(i));
        if (!result.isNull(i)) {
            CastString::castToFixedList(
                inputVector->getValue<ku_string_t>(i), &result, i, csvReaderConfig);
        }
    }
}

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

static bool containsListToFixedList(const LogicalType* srcType, const LogicalType* dstType) {
    if (srcType->getLogicalTypeID() == LogicalTypeID::VAR_LIST &&
        dstType->getLogicalTypeID() == LogicalTypeID::FIXED_LIST) {
        return true;
    }

    while (srcType->getLogicalTypeID() == dstType->getLogicalTypeID()) {
        switch (srcType->getPhysicalType()) {
        case PhysicalTypeID::VAR_LIST: {
            return containsListToFixedList(
                VarListType::getChildType(srcType), VarListType::getChildType(dstType));
        }
        case PhysicalTypeID::STRUCT: {
            auto srcFieldTypes = StructType::getFieldTypes(srcType);
            auto dstFieldTypes = StructType::getFieldTypes(dstType);
            if (srcFieldTypes.size() != dstFieldTypes.size()) {
                throw ConversionException{
                    stringFormat("Unsupported casting function from {} to {}.", srcType->toString(),
                        dstType->toString())};
            }

            auto result = false;
            std::vector<struct_field_idx_t> fields;
            for (auto i = 0u; i < srcFieldTypes.size(); i++) {
                if (containsListToFixedList(srcFieldTypes[i], dstFieldTypes[i])) {
                    return true;
                }
            }
        }
        default:
            return false;
        }
    }
    return false;
}

static void validateListEntry(ValueVector* inputVector, LogicalType* resultType, uint64_t pos) {
    if (inputVector->isNull(pos)) {
        return;
    }

    switch (resultType->getPhysicalType()) {
    case PhysicalTypeID::FIXED_LIST: {
        auto listEntry = inputVector->getValue<list_entry_t>(pos);
        if (listEntry.size != FixedListType::getNumValuesInList(resultType)) {
            throw ConversionException{stringFormat(
                "Unsupported casting VAR_LIST with incorrect list entry to FIXED_LIST. "
                "Expected: {}, Actual: {}.",
                FixedListType::getNumValuesInList(resultType),
                inputVector->getValue<list_entry_t>(pos).size)};
        }

        auto inputChildVector = ListVector::getDataVector(inputVector);
        for (auto i = listEntry.offset; i < listEntry.offset + listEntry.size; i++) {
            if (inputChildVector->isNull(i)) {
                throw ConversionException("Cast failed. NULL is not allowed for FIXED_LIST.");
            }
        }
    } break;
    case PhysicalTypeID::VAR_LIST: {
        auto listEntry = inputVector->getValue<list_entry_t>(pos);
        for (auto i = listEntry.offset; i < listEntry.offset + listEntry.size; i++) {
            validateListEntry(
                ListVector::getDataVector(inputVector), VarListType::getChildType(resultType), i);
        }
    } break;
    case PhysicalTypeID::STRUCT: {
        auto fieldVectors = StructVector::getFieldVectors(inputVector);
        auto fieldTypes = StructType::getFieldTypes(resultType);

        auto structEntry = inputVector->getValue<struct_entry_t>(pos);
        for (auto i = 0u; i < fieldVectors.size(); i++) {
            validateListEntry(fieldVectors[i].get(), fieldTypes[i], structEntry.pos);
        }
    } break;
    default: {
        return;
    }
    }
}

template<typename /*EXECUTOR*/ = UnaryFunctionExecutor>
static void listToFixedListCastExecFunction(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr) {
    KU_ASSERT(params.size() == 1);
    auto inputVector = params[0];

    for (auto i = 0u; i < inputVector->state->selVector->selectedSize; i++) {
        auto pos = inputVector->state->selVector->selectedPositions[i];
        validateListEntry(inputVector.get(), &result.dataType, pos);
    }

    auto numOfEntries = inputVector->state->selVector
                            ->selectedPositions[inputVector->state->selVector->selectedSize - 1] +
                        1;
    reinterpret_cast<CastFunctionBindData*>(dataPtr)->numOfEntries = numOfEntries;
    listToFixedListCastExecFunction<CastChildFunctionExecutor>(params, result, dataPtr);
}

using scalar_cast_func = std::function<void(void*, uint64_t, void*, uint64_t, void*)>;

template<typename DST_TYPE, typename OP>
static void getFixedListChildFuncHelper(scalar_cast_func& func, LogicalTypeID inputTypeID) {
    switch (inputTypeID) {
    case LogicalTypeID::STRING: {
        func = UnaryCastStringFunctionWrapper::operation<ku_string_t, DST_TYPE, CastString>;
    } break;
    case LogicalTypeID::INT128: {
        func = UnaryFunctionWrapper::operation<int128_t, DST_TYPE, OP>;
    } break;
    case LogicalTypeID::INT64: {
        func = UnaryFunctionWrapper::operation<int64_t, DST_TYPE, OP>;
    } break;
    case LogicalTypeID::INT32: {
        func = UnaryFunctionWrapper::operation<int32_t, DST_TYPE, OP>;
    } break;
    case LogicalTypeID::INT16: {
        func = UnaryFunctionWrapper::operation<int16_t, DST_TYPE, OP>;
    } break;
    case LogicalTypeID::INT8: {
        func = UnaryFunctionWrapper::operation<int8_t, DST_TYPE, OP>;
    } break;
    case LogicalTypeID::UINT8: {
        func = UnaryFunctionWrapper::operation<uint8_t, DST_TYPE, OP>;
    } break;
    case LogicalTypeID::UINT16: {
        func = UnaryFunctionWrapper::operation<uint16_t, DST_TYPE, OP>;
    } break;
    case LogicalTypeID::UINT32: {
        func = UnaryFunctionWrapper::operation<uint32_t, DST_TYPE, OP>;
    } break;
    case LogicalTypeID::UINT64: {
        func = UnaryFunctionWrapper::operation<uint64_t, DST_TYPE, OP>;
    } break;
    case LogicalTypeID::FLOAT: {
        func = UnaryFunctionWrapper::operation<float_t, DST_TYPE, OP>;
    } break;
    case LogicalTypeID::DOUBLE: {
        func = UnaryFunctionWrapper::operation<double_t, DST_TYPE, OP>;
    } break;
    default: {
        throw ConversionException{
            stringFormat("Unsupported casting function from {} to numerical type.",
                LogicalTypeUtils::toString(inputTypeID))};
    }
    }
}

static void getFixedListChildCastFunc(
    scalar_cast_func& func, LogicalTypeID inputType, LogicalTypeID resultType) {
    // only support limited Fixed List Types
    switch (resultType) {
    case LogicalTypeID::INT64: {
        return getFixedListChildFuncHelper<int64_t, CastToInt64>(func, inputType);
    }
    case LogicalTypeID::INT32: {
        return getFixedListChildFuncHelper<int32_t, CastToInt32>(func, inputType);
    }
    case LogicalTypeID::INT16: {
        return getFixedListChildFuncHelper<int16_t, CastToInt16>(func, inputType);
    }
    case LogicalTypeID::DOUBLE: {
        return getFixedListChildFuncHelper<double_t, CastToDouble>(func, inputType);
    }
    case LogicalTypeID::FLOAT: {
        return getFixedListChildFuncHelper<float_t, CastToFloat>(func, inputType);
    }
    default: {
        throw RuntimeException("Unsupported FIXED_LIST type: Function::getFixedListChildCastFunc");
    }
    }
}

template<>
void listToFixedListCastExecFunction<CastChildFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr) {
    auto inputVector = params[0];
    auto numOfEntries = reinterpret_cast<CastFunctionBindData*>(dataPtr)->numOfEntries;

    auto inputChildId = VarListType::getChildType(&inputVector->dataType)->getLogicalTypeID();
    auto outputChildID = FixedListType::getChildType(&result.dataType)->getLogicalTypeID();
    auto numValuesPerList = FixedListType::getNumValuesInList(&result.dataType);
    scalar_cast_func func;
    getFixedListChildCastFunc(func, inputChildId, outputChildID);

    result.setNullFromBits(inputVector->getNullMaskData(), 0, 0, numOfEntries);
    auto inputChildVector = ListVector::getDataVector(inputVector.get());
    for (auto i = 0u; i < numOfEntries; i++) {
        if (!result.isNull(i)) {
            auto listEntry = inputVector->getValue<list_entry_t>(i);
            if (listEntry.size == numValuesPerList) {
                for (auto j = 0u; j < listEntry.size; j++) {
                    func((void*)(inputChildVector), listEntry.offset + j, (void*)(&result),
                        i * numValuesPerList + j, nullptr);
                }
            }
        }
    }
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
    if (containsListToFixedList(&inputVector->dataType, &result.dataType)) {
        for (auto i = 0u; i < inputVector->state->selVector->selectedSize; i++) {
            auto pos = inputVector->state->selVector->selectedPositions[i];
            validateListEntry(inputVector.get(), &result.dataType, pos);
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
        execFunc = stringtoFixedListCastExecFunction<EXECUTOR>;
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
        func = fixedListToStringCastExecFunction<EXECUTOR>;
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
                listToFixedListCastExecFunction<EXECUTOR>);
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
        }
    }
    default:
        // lcov_excl_start
        // TODO(kebing): implement more
        throw ConversionException{stringFormat("Unsupported casting function from {} to {}.",
            LogicalTypeUtils::toString(sourceTypeID), LogicalTypeUtils::toString(targetTypeID))};
        // lcov_excl_end
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
