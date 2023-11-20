#include "function/cast/functions/cast_fixed_list.h"

#include "common/exception/conversion.h"
#include "common/type_utils.h"
#include "function/cast/functions/cast_from_string_functions.h"
#include "function/cast/functions/cast_functions.h"

namespace kuzu {
namespace function {

bool CastFixedListHelper::containsListToFixedList(
    const LogicalType* srcType, const LogicalType* dstType) {
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

void CastFixedListHelper::validateListEntry(
    ValueVector* inputVector, LogicalType* resultType, uint64_t pos) {
    if (inputVector->isNull(pos)) {
        return;
    }
    auto inputTypeID = inputVector->dataType.getPhysicalType();

    switch (resultType->getPhysicalType()) {
    case PhysicalTypeID::FIXED_LIST: {
        if (inputTypeID == PhysicalTypeID::VAR_LIST) {
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
        }
    } break;
    case PhysicalTypeID::VAR_LIST: {
        if (inputTypeID == PhysicalTypeID::VAR_LIST) {
            auto listEntry = inputVector->getValue<list_entry_t>(pos);
            for (auto i = listEntry.offset; i < listEntry.offset + listEntry.size; i++) {
                validateListEntry(ListVector::getDataVector(inputVector),
                    VarListType::getChildType(resultType), i);
            }
        }
    } break;
    case PhysicalTypeID::STRUCT: {
        if (inputTypeID == PhysicalTypeID::STRUCT) {
            auto fieldVectors = StructVector::getFieldVectors(inputVector);
            auto fieldTypes = StructType::getFieldTypes(resultType);

            auto structEntry = inputVector->getValue<struct_entry_t>(pos);
            for (auto i = 0u; i < fieldVectors.size(); i++) {
                validateListEntry(fieldVectors[i].get(), fieldTypes[i], structEntry.pos);
            }
        }
    } break;
    default: {
        return;
    }
    }
}

static void CastFixedListToString(
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

template<>
void CastFixedList::fixedListToStringCastExecFunction<UnaryFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result,
    void* /*dataPtr*/) {
    KU_ASSERT(params.size() == 1);
    auto param = params[0];
    if (param->state->isFlat()) {
        CastFixedListToString(*param, param->state->selVector->selectedPositions[0], result,
            result.state->selVector->selectedPositions[0]);
    } else if (param->state->selVector->isUnfiltered()) {
        for (auto i = 0u; i < param->state->selVector->selectedSize; i++) {
            CastFixedListToString(*param, i, result, i);
        }
    } else {
        for (auto i = 0u; i < param->state->selVector->selectedSize; i++) {
            CastFixedListToString(*param, param->state->selVector->selectedPositions[i], result,
                result.state->selVector->selectedPositions[i]);
        }
    }
}

// LCOV_EXCL_START
template<>
void CastFixedList::fixedListToStringCastExecFunction<CastFixedListToListFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& /*params*/, ValueVector& /*result*/,
    void* /*dataPtr*/) {
    KU_UNREACHABLE;
}
// LCOV_EXCL_STOP

template<>
void CastFixedList::fixedListToStringCastExecFunction<CastChildFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr) {
    KU_ASSERT(params.size() == 1);

    auto inputVector = params[0].get();
    auto numOfEntries = reinterpret_cast<CastFunctionBindData*>(dataPtr)->numOfEntries;
    for (auto i = 0u; i < numOfEntries; i++) {
        CastFixedListToString(*inputVector, i, result, i);
    }
}

template<>
void CastFixedList::stringtoFixedListCastExecFunction<UnaryFunctionExecutor>(
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

// LCOV_EXCL_START
template<>
void CastFixedList::stringtoFixedListCastExecFunction<CastFixedListToListFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& /*params*/, ValueVector& /*result*/,
    void* /*dataPtr*/) {
    KU_UNREACHABLE;
}
// LCOV_EXCL_STOP

template<>
void CastFixedList::stringtoFixedListCastExecFunction<CastChildFunctionExecutor>(
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

template<>
void CastFixedList::listToFixedListCastExecFunction<UnaryFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr) {
    KU_ASSERT(params.size() == 1);
    auto inputVector = params[0];

    for (auto i = 0u; i < inputVector->state->selVector->selectedSize; i++) {
        auto pos = inputVector->state->selVector->selectedPositions[i];
        CastFixedListHelper::validateListEntry(inputVector.get(), &result.dataType, pos);
    }

    auto numOfEntries = inputVector->state->selVector
                            ->selectedPositions[inputVector->state->selVector->selectedSize - 1] +
                        1;
    reinterpret_cast<CastFunctionBindData*>(dataPtr)->numOfEntries = numOfEntries;
    listToFixedListCastExecFunction<CastChildFunctionExecutor>(params, result, dataPtr);
}

// LCOV_EXCL_START
template<>
void CastFixedList::listToFixedListCastExecFunction<CastFixedListToListFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& /*params*/, ValueVector& /*result*/,
    void* /*dataPtr*/) {
    KU_UNREACHABLE;
}
// LCOV_EXCL_STOP

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
void CastFixedList::listToFixedListCastExecFunction<CastChildFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr) {
    auto inputVector = params[0];
    auto numOfEntries = reinterpret_cast<CastFunctionBindData*>(dataPtr)->numOfEntries;

    auto inputChildId = VarListType::getChildType(&inputVector->dataType)->getLogicalTypeID();
    auto outputChildId = FixedListType::getChildType(&result.dataType)->getLogicalTypeID();
    auto numValuesPerList = FixedListType::getNumValuesInList(&result.dataType);
    scalar_cast_func func;
    getFixedListChildCastFunc(func, inputChildId, outputChildId);

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

template<>
void CastFixedList::castBetweenFixedListExecFunc<UnaryFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr) {
    auto inputVector = params[0];
    auto numOfEntries = inputVector->state->selVector
                            ->selectedPositions[inputVector->state->selVector->selectedSize - 1] +
                        1;
    reinterpret_cast<CastFunctionBindData*>(dataPtr)->numOfEntries = numOfEntries;
    castBetweenFixedListExecFunc<CastChildFunctionExecutor>(params, result, dataPtr);
}

// LCOV_EXCL_START
template<>
void CastFixedList::castBetweenFixedListExecFunc<CastFixedListToListFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& /*params*/, ValueVector& /*result*/,
    void* /*dataPtr*/) {
    KU_UNREACHABLE;
}
// LCOV_EXCL_STOP

template<>
void CastFixedList::castBetweenFixedListExecFunc<CastChildFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr) {
    auto inputVector = params[0];
    auto numOfEntries = reinterpret_cast<CastFunctionBindData*>(dataPtr)->numOfEntries;

    auto inputChildId = FixedListType::getChildType(&inputVector->dataType)->getLogicalTypeID();
    auto outputChildId = FixedListType::getChildType(&result.dataType)->getLogicalTypeID();
    auto numValuesPerList = FixedListType::getNumValuesInList(&result.dataType);
    if (FixedListType::getNumValuesInList(&inputVector->dataType) != numValuesPerList) {
        throw ConversionException(stringFormat("Unsupported casting function from {} to {}.",
            inputVector->dataType.toString(), result.dataType.toString()));
    }

    scalar_cast_func func;
    getFixedListChildCastFunc(func, inputChildId, outputChildId);

    result.setNullFromBits(inputVector->getNullMaskData(), 0, 0, numOfEntries);
    for (auto i = 0u; i < numOfEntries; i++) {
        if (!result.isNull(i)) {
            for (auto j = 0u; j < numValuesPerList; j++) {
                func((void*)(inputVector.get()), i * numValuesPerList + j, (void*)(&result),
                    i * numValuesPerList + j, nullptr);
            }
        }
    }
}

} // namespace function
} // namespace kuzu
