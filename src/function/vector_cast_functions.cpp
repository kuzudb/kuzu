#include "function/cast/vector_cast_functions.h"

#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "common/exception/binder.h"
#include "common/exception/conversion.h"
#include "function/built_in_function_utils.h"
#include "function/cast/functions/cast_array.h"
#include "function/cast/functions/cast_decimal.h"
#include "function/cast/functions/cast_from_string_functions.h"
#include "function/cast/functions/cast_functions.h"
#include "function/cast/functions/cast_rdf_variant.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace function {

struct CastChildFunctionExecutor {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC, typename OP_WRAPPER>
    static void executeSwitch(common::ValueVector& operand, common::ValueVector& result,
        void* dataPtr) {
        auto numOfEntries = reinterpret_cast<CastFunctionBindData*>(dataPtr)->numOfEntries;
        for (auto i = 0u; i < numOfEntries; i++) {
            result.setNull(i, operand.isNull(i));
            if (!result.isNull(i)) {
                OP_WRAPPER::template operation<OPERAND_TYPE, RESULT_TYPE, FUNC>((void*)(&operand),
                    i, (void*)(&result), i, dataPtr);
            }
        }
    }
};

static void resolveNestedVector(std::shared_ptr<ValueVector> inputVector, ValueVector* resultVector,
    uint64_t numOfEntries, CastFunctionBindData* dataPtr) {
    const auto* inputType = &inputVector->dataType;
    const auto* resultType = &resultVector->dataType;
    while (true) {
        if ((inputType->getPhysicalType() == PhysicalTypeID::LIST ||
                inputType->getPhysicalType() == PhysicalTypeID::ARRAY) &&
            (resultType->getPhysicalType() == PhysicalTypeID::LIST ||
                resultType->getPhysicalType() == PhysicalTypeID::ARRAY)) {
            // copy data and nullmask from input
            memcpy(resultVector->getData(), inputVector->getData(),
                numOfEntries * resultVector->getNumBytesPerValue());
            resultVector->setNullFromBits(inputVector->getNullMask().getData(), 0, 0, numOfEntries);

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
            auto inputTypeNames = StructType::getFieldNames(*inputType);
            auto resultTypeNames = StructType::getFieldNames(*resultType);
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
            resultVector->setNullFromBits(inputVector->getNullMask().getData(), 0, 0, numOfEntries);

            auto inputFieldVectors = StructVector::getFieldVectors(inputVector.get());
            auto resultFieldVectors = StructVector::getFieldVectors(resultVector);
            for (auto i = 0u; i < inputFieldVectors.size(); i++) {
                resolveNestedVector(inputFieldVectors[i], resultFieldVectors[i].get(), numOfEntries,
                    dataPtr);
            }
            return;
        } else {
            break;
        }
    }

    // non-nested types
    if (inputType->getLogicalTypeID() != resultType->getLogicalTypeID()) {
        auto func = CastFunction::bindCastFunction<CastChildFunctionExecutor>("CAST", *inputType,
            *resultType)
                        ->execFunc;
        std::vector<std::shared_ptr<ValueVector>> childParams{inputVector};
        dataPtr->numOfEntries = numOfEntries;
        func(childParams, *resultVector, (void*)dataPtr);
    } else {
        for (auto i = 0u; i < numOfEntries; i++) {
            resultVector->copyFromVectorData(i, inputVector.get(), i);
        }
    }
}

static void nestedTypesCastExecFunction(const std::vector<std::shared_ptr<ValueVector>>& params,
    ValueVector& result, void*) {
    KU_ASSERT(params.size() == 1);
    result.resetAuxiliaryBuffer();
    const auto& inputVector = params[0];

    // check if all selcted list entry have the requried fixed list size
    if (CastArrayHelper::containsListToArray(inputVector->dataType, result.dataType)) {
        for (auto i = 0u; i < inputVector->state->getSelVector().getSelSize(); i++) {
            auto pos = inputVector->state->getSelVector()[i];
            CastArrayHelper::validateListEntry(inputVector.get(), result.dataType, pos);
        }
    };

    auto& selVector = inputVector->state->getSelVector();
    auto bindData = CastFunctionBindData(result.dataType.copy());
    auto numOfEntries = selVector[selVector.getSelSize() - 1] + 1;
    resolveNestedVector(inputVector, &result, numOfEntries, &bindData);
    if (inputVector->state->isFlat()) {
        result.state->getSelVectorUnsafe().setToFiltered();
        result.state->getSelVectorUnsafe()[0] = inputVector->state->getSelVector()[0];
    }
}

static bool hasImplicitCastList(const LogicalType& srcType, const LogicalType& dstType) {
    return CastFunction::hasImplicitCast(ListType::getChildType(srcType),
        ListType::getChildType(dstType));
}

static bool hasImplicitCastArray(const LogicalType& srcType, const LogicalType& dstType) {
    if (ArrayType::getNumElements(srcType) != ArrayType::getNumElements(dstType)) {
        return false;
    }
    return CastFunction::hasImplicitCast(ArrayType::getChildType(srcType),
        ArrayType::getChildType(dstType));
}

static bool hasImplicitCastArrayToList(const LogicalType& srcType, const LogicalType& dstType) {
    return CastFunction::hasImplicitCast(ArrayType::getChildType(srcType),
        ListType::getChildType(dstType));
}

static bool hasImplicitCastListToArray(const LogicalType& srcType, const LogicalType& dstType) {
    return CastFunction::hasImplicitCast(ListType::getChildType(srcType),
        ArrayType::getChildType(dstType));
}

static bool hasImplicitCastStruct(const LogicalType& srcType, const LogicalType& dstType) {
    const auto& srcFields = StructType::getFields(srcType);
    const auto& dstFields = StructType::getFields(dstType);
    if (srcFields.size() != dstFields.size()) {
        return false;
    }
    for (auto i = 0u; i < srcFields.size(); i++) {
        if (srcFields[i].getName() != dstFields[i].getName()) {
            return false;
        }
        if (!CastFunction::hasImplicitCast(srcFields[i].getType(), dstFields[i].getType())) {
            return false;
        }
    }
    return true;
}

static bool hasImplicitCastUnion(const LogicalType& /*srcType*/, const LogicalType& /*dstType*/) {
    // todo: implement union casting function
    // currently, there seems to be no casting functionality between union types
    return false;
}

static bool hasImplicitCastMap(const LogicalType& srcType, const LogicalType& dstType) {
    const auto& srcKeyType = MapType::getKeyType(srcType);
    const auto& srcValueType = MapType::getValueType(srcType);
    const auto& dstKeyType = MapType::getKeyType(dstType);
    const auto& dstValueType = MapType::getValueType(dstType);
    return CastFunction::hasImplicitCast(srcKeyType, dstKeyType) &&
           CastFunction::hasImplicitCast(srcValueType, dstValueType);
}

bool CastFunction::hasImplicitCast(const LogicalType& srcType, const LogicalType& dstType) {
    if ((LogicalTypeUtils::isNested(srcType) &&
            srcType.getLogicalTypeID() != LogicalTypeID::RDF_VARIANT) &&
        (LogicalTypeUtils::isNested(dstType) &&
            dstType.getLogicalTypeID() != LogicalTypeID::RDF_VARIANT)) {
        if (srcType.getLogicalTypeID() == LogicalTypeID::ARRAY &&
            dstType.getLogicalTypeID() == LogicalTypeID::LIST) {
            return hasImplicitCastArrayToList(srcType, dstType);
        }
        if (srcType.getLogicalTypeID() == LogicalTypeID::LIST &&
            dstType.getLogicalTypeID() == LogicalTypeID::ARRAY) {
            return hasImplicitCastListToArray(srcType, dstType);
        }
        if (srcType.getLogicalTypeID() != dstType.getLogicalTypeID()) {
            return false;
        }
        switch (srcType.getLogicalTypeID()) {
        case LogicalTypeID::LIST:
            return hasImplicitCastList(srcType, dstType);
        case LogicalTypeID::ARRAY:
            return hasImplicitCastArray(srcType, dstType);
        case LogicalTypeID::STRUCT:
            return hasImplicitCastStruct(srcType, dstType);
        case LogicalTypeID::UNION:
            return hasImplicitCastUnion(srcType, dstType);
        case LogicalTypeID::MAP:
            return hasImplicitCastMap(srcType, dstType);
        default:
            // LCOV_EXCL_START
            KU_UNREACHABLE;
            // LCOV_EXCL_END
        }
    }
    if (BuiltInFunctionsUtils::getCastCost(srcType.getLogicalTypeID(),
            dstType.getLogicalTypeID()) != UNDEFINED_CAST_COST) {
        return true;
    }
    // TODO(Jiamin): there are still other special cases
    // We allow cast between any numerical types
    if (LogicalTypeUtils::isNumerical(srcType) && LogicalTypeUtils::isNumerical(dstType)) {
        return true;
    }
    return false;
}

template<typename EXECUTOR = UnaryFunctionExecutor>
static std::unique_ptr<ScalarFunction> bindCastFromStringFunction(const std::string& functionName,
    const LogicalType& targetType) {
    scalar_func_exec_t execFunc;
    switch (targetType.getLogicalTypeID()) {
    case LogicalTypeID::DATE: {
        execFunc =
            ScalarFunction::UnaryCastStringExecFunction<ku_string_t, date_t, CastString, EXECUTOR>;
    } break;
    case LogicalTypeID::TIMESTAMP_SEC: {
        execFunc = ScalarFunction::UnaryCastStringExecFunction<ku_string_t, timestamp_sec_t,
            CastString, EXECUTOR>;
    } break;
    case LogicalTypeID::TIMESTAMP_MS: {
        execFunc = ScalarFunction::UnaryCastStringExecFunction<ku_string_t, timestamp_ms_t,
            CastString, EXECUTOR>;
    } break;
    case LogicalTypeID::TIMESTAMP_NS: {
        execFunc = ScalarFunction::UnaryCastStringExecFunction<ku_string_t, timestamp_ns_t,
            CastString, EXECUTOR>;
    } break;
    case LogicalTypeID::TIMESTAMP_TZ: {
        execFunc = ScalarFunction::UnaryCastStringExecFunction<ku_string_t, timestamp_tz_t,
            CastString, EXECUTOR>;
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
    case LogicalTypeID::UUID: {
        execFunc = ScalarFunction::UnaryCastStringExecFunction<ku_string_t, ku_uuid_t, CastString,
            EXECUTOR>;
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
        execFunc =
            ScalarFunction::UnaryCastStringExecFunction<ku_string_t, double, CastString, EXECUTOR>;
    } break;
    case LogicalTypeID::FLOAT: {
        execFunc =
            ScalarFunction::UnaryCastStringExecFunction<ku_string_t, float, CastString, EXECUTOR>;
    } break;
    case LogicalTypeID::DECIMAL: {
        switch (targetType.getPhysicalType()) {
        case PhysicalTypeID::INT16:
            execFunc =
                ScalarFunction::UnaryExecNestedTypeFunction<ku_string_t, int16_t, CastToDecimal>;
            break;
        case PhysicalTypeID::INT32:
            execFunc =
                ScalarFunction::UnaryExecNestedTypeFunction<ku_string_t, int32_t, CastToDecimal>;
            break;
        case PhysicalTypeID::INT64:
            execFunc =
                ScalarFunction::UnaryExecNestedTypeFunction<ku_string_t, int64_t, CastToDecimal>;
            break;
        case PhysicalTypeID::INT128:
            execFunc =
                ScalarFunction::UnaryExecNestedTypeFunction<ku_string_t, int128_t, CastToDecimal>;
            break;
        default:
            KU_UNREACHABLE;
        }
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
    case LogicalTypeID::ARRAY:
    case LogicalTypeID::LIST: {
        execFunc = ScalarFunction::UnaryCastStringExecFunction<ku_string_t, list_entry_t,
            CastString, EXECUTOR>;
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
    case LogicalTypeID::RDF_VARIANT: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<ku_string_t, struct_entry_t,
            CastToRdfVariant, UnaryFunctionExecutor>;
    } break;
    default:
        throw ConversionException{
            stringFormat("Unsupported casting function from STRING to {}.", targetType.toString())};
    }
    return std::make_unique<ScalarFunction>(functionName,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, targetType.getLogicalTypeID(), execFunc);
}

static std::unique_ptr<ScalarFunction> bindCastFromRdfVariantFunction(
    const std::string& functionName, const LogicalType& targetType) {
    scalar_func_exec_t execFunc;
    switch (targetType.getLogicalTypeID()) {
    case LogicalTypeID::DATE: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<struct_entry_t, date_t,
            CastFromRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::TIMESTAMP_NS:
    case LogicalTypeID::TIMESTAMP_MS:
    case LogicalTypeID::TIMESTAMP_SEC:
    case LogicalTypeID::TIMESTAMP_TZ:
    case LogicalTypeID::TIMESTAMP: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<struct_entry_t, timestamp_t,
            CastFromRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::INTERVAL: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<struct_entry_t, interval_t,
            CastFromRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::BLOB: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<struct_entry_t, blob_t,
            CastFromRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::STRING: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<struct_entry_t, ku_string_t,
            CastFromRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::BOOL: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<struct_entry_t, bool,
            CastFromRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::DOUBLE: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<struct_entry_t, double,
            CastFromRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::FLOAT: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<struct_entry_t, float,
            CastFromRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<struct_entry_t, int64_t,
            CastFromRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::INT32: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<struct_entry_t, int32_t,
            CastFromRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::INT16: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<struct_entry_t, int16_t,
            CastFromRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::INT8: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<struct_entry_t, int8_t,
            CastFromRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::UINT64: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<struct_entry_t, uint64_t,
            CastFromRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::UINT32: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<struct_entry_t, uint32_t,
            CastFromRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::UINT16: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<struct_entry_t, uint16_t,
            CastFromRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::UINT8: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<struct_entry_t, uint8_t,
            CastFromRdfVariant, UnaryFunctionExecutor>;
    } break;
        // LCOV_EXCL_START
    default:
        throw ConversionException{stringFormat(
            "Unsupported casting function from RDF_VARIANT to {}.", targetType.toString())};
        // LCOV_EXCL_STOP
    }
    return std::make_unique<ScalarFunction>(functionName,
        std::vector<LogicalTypeID>{LogicalTypeID::RDF_VARIANT}, targetType.getLogicalTypeID(),
        execFunc);
}

template<typename EXECUTOR = UnaryFunctionExecutor>
static std::unique_ptr<ScalarFunction> bindCastToStringFunction(const std::string& functionName,
    const LogicalType& sourceType) {
    scalar_func_exec_t func;
    switch (sourceType.getLogicalTypeID()) {
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
        func = ScalarFunction::UnaryCastExecFunction<double, ku_string_t, CastToString, EXECUTOR>;
    } break;
    case LogicalTypeID::FLOAT: {
        func = ScalarFunction::UnaryCastExecFunction<float, ku_string_t, CastToString, EXECUTOR>;
    } break;
    case LogicalTypeID::DECIMAL: {
        switch (sourceType.getPhysicalType()) {
        case PhysicalTypeID::INT16:
            func = ScalarFunction::UnaryExecNestedTypeFunction<int16_t, ku_string_t, CastDecimalTo>;
            break;
        case PhysicalTypeID::INT32:
            func = ScalarFunction::UnaryExecNestedTypeFunction<int32_t, ku_string_t, CastDecimalTo>;
            break;
        case PhysicalTypeID::INT64:
            func = ScalarFunction::UnaryExecNestedTypeFunction<int64_t, ku_string_t, CastDecimalTo>;
            break;
        case PhysicalTypeID::INT128:
            func =
                ScalarFunction::UnaryExecNestedTypeFunction<int128_t, ku_string_t, CastDecimalTo>;
            break;
        default:
            KU_UNREACHABLE;
        }
    } break;
    case LogicalTypeID::DATE: {
        func = ScalarFunction::UnaryCastExecFunction<date_t, ku_string_t, CastToString, EXECUTOR>;
    } break;
    case LogicalTypeID::TIMESTAMP_NS: {
        func = ScalarFunction::UnaryCastExecFunction<timestamp_ns_t, ku_string_t, CastToString,
            EXECUTOR>;
    } break;
    case LogicalTypeID::TIMESTAMP_MS: {
        func = ScalarFunction::UnaryCastExecFunction<timestamp_ms_t, ku_string_t, CastToString,
            EXECUTOR>;
    } break;
    case LogicalTypeID::TIMESTAMP_SEC: {
        func = ScalarFunction::UnaryCastExecFunction<timestamp_sec_t, ku_string_t, CastToString,
            EXECUTOR>;
    } break;
    case LogicalTypeID::TIMESTAMP_TZ: {
        func = ScalarFunction::UnaryCastExecFunction<timestamp_tz_t, ku_string_t, CastToString,
            EXECUTOR>;
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
    case LogicalTypeID::UUID: {
        func =
            ScalarFunction::UnaryCastExecFunction<ku_uuid_t, ku_string_t, CastToString, EXECUTOR>;
    } break;
    case LogicalTypeID::ARRAY:
    case LogicalTypeID::LIST: {
        func = ScalarFunction::UnaryCastExecFunction<list_entry_t, ku_string_t, CastToString,
            EXECUTOR>;
    } break;
    case LogicalTypeID::MAP: {
        func =
            ScalarFunction::UnaryCastExecFunction<map_entry_t, ku_string_t, CastToString, EXECUTOR>;
    } break;
    case LogicalTypeID::NODE: {
        func = ScalarFunction::UnaryCastExecFunction<struct_entry_t, ku_string_t, CastNodeToString,
            EXECUTOR>;
    } break;
    case LogicalTypeID::REL: {
        func = ScalarFunction::UnaryCastExecFunction<struct_entry_t, ku_string_t, CastRelToString,
            EXECUTOR>;
    } break;
    case LogicalTypeID::RECURSIVE_REL:
    case LogicalTypeID::STRUCT: {
        func = ScalarFunction::UnaryCastExecFunction<struct_entry_t, ku_string_t, CastToString,
            EXECUTOR>;
    } break;
    case LogicalTypeID::UNION: {
        func = ScalarFunction::UnaryCastExecFunction<union_entry_t, ku_string_t, CastToString,
            EXECUTOR>;
    } break;
    default:
        KU_UNREACHABLE;
    }
    return std::make_unique<ScalarFunction>(functionName,
        std::vector<LogicalTypeID>{sourceType.getLogicalTypeID()}, LogicalTypeID::STRING, func);
}

static std::unique_ptr<ScalarFunction> bindCastToRdfVariantFunction(const std::string& functionName,
    const LogicalType& sourceType) {
    scalar_func_exec_t execFunc;
    switch (sourceType.getLogicalTypeID()) {
    case LogicalTypeID::DATE: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<date_t, struct_entry_t,
            CastToRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::TIMESTAMP_NS:
    case LogicalTypeID::TIMESTAMP_MS:
    case LogicalTypeID::TIMESTAMP_SEC:
    case LogicalTypeID::TIMESTAMP_TZ:
    case LogicalTypeID::TIMESTAMP: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<timestamp_t, struct_entry_t,
            CastToRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::INTERVAL: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<interval_t, struct_entry_t,
            CastToRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::BLOB: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<blob_t, struct_entry_t,
            CastToRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::STRING: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<ku_string_t, struct_entry_t,
            CastToRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::BOOL: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<bool, struct_entry_t,
            CastToRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::DOUBLE: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<double, struct_entry_t,
            CastToRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::FLOAT: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<float, struct_entry_t,
            CastToRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<int64_t, struct_entry_t,
            CastToRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::INT32: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<int32_t, struct_entry_t,
            CastToRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::INT16: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<int16_t, struct_entry_t,
            CastToRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::INT8: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<int8_t, struct_entry_t,
            CastToRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::UINT64: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<uint64_t, struct_entry_t,
            CastToRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::UINT32: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<uint32_t, struct_entry_t,
            CastToRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::UINT16: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<uint16_t, struct_entry_t,
            CastToRdfVariant, UnaryFunctionExecutor>;
    } break;
    case LogicalTypeID::UINT8: {
        execFunc = ScalarFunction::UnaryRdfVariantCastExecFunction<uint8_t, struct_entry_t,
            CastToRdfVariant, UnaryFunctionExecutor>;
    } break;
        // LCOV_EXCL_START
    default:
        throw ConversionException{
            stringFormat("Unsupported casting function from RDF_VARIANT to .")};
        // LCOV_EXCL_STOP
    }
    return std::make_unique<ScalarFunction>(functionName,
        std::vector<LogicalTypeID>{sourceType.getLogicalTypeID()}, LogicalTypeID::RDF_VARIANT,
        execFunc);
}

template<typename DST_TYPE>
static std::unique_ptr<ScalarFunction> bindCastToDecimalFunction(const std::string& functionName,
    const LogicalType& sourceType, const LogicalType& targetType) {
    scalar_func_exec_t func;
    switch (sourceType.getLogicalTypeID()) {
    case LogicalTypeID::INT8: {
        func = ScalarFunction::UnaryExecNestedTypeFunction<int8_t, DST_TYPE, CastToDecimal>;
    } break;
    case LogicalTypeID::INT16: {
        func = ScalarFunction::UnaryExecNestedTypeFunction<int16_t, DST_TYPE, CastToDecimal>;
    } break;
    case LogicalTypeID::INT32: {
        func = ScalarFunction::UnaryExecNestedTypeFunction<int32_t, DST_TYPE, CastToDecimal>;
    } break;
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        func = ScalarFunction::UnaryExecNestedTypeFunction<int64_t, DST_TYPE, CastToDecimal>;
    } break;
    case LogicalTypeID::UINT8: {
        func = ScalarFunction::UnaryExecNestedTypeFunction<uint8_t, DST_TYPE, CastToDecimal>;
    } break;
    case LogicalTypeID::UINT16: {
        func = ScalarFunction::UnaryExecNestedTypeFunction<uint16_t, DST_TYPE, CastToDecimal>;
    } break;
    case LogicalTypeID::UINT32: {
        func = ScalarFunction::UnaryExecNestedTypeFunction<uint32_t, DST_TYPE, CastToDecimal>;
    } break;
    case LogicalTypeID::UINT64: {
        func = ScalarFunction::UnaryExecNestedTypeFunction<uint64_t, DST_TYPE, CastToDecimal>;
    } break;
    case LogicalTypeID::INT128: {
        func = ScalarFunction::UnaryExecNestedTypeFunction<int128_t, DST_TYPE, CastToDecimal>;
    } break;
    case LogicalTypeID::FLOAT: {
        func = ScalarFunction::UnaryExecNestedTypeFunction<float, DST_TYPE, CastToDecimal>;
    } break;
    case LogicalTypeID::DOUBLE: {
        func = ScalarFunction::UnaryExecNestedTypeFunction<double, DST_TYPE, CastToDecimal>;
    } break;
    case LogicalTypeID::DECIMAL: {
        switch (sourceType.getPhysicalType()) {
        case PhysicalTypeID::INT16:
            func =
                ScalarFunction::UnaryExecNestedTypeFunction<int16_t, DST_TYPE, CastBetweenDecimal>;
            break;
        case PhysicalTypeID::INT32:
            func =
                ScalarFunction::UnaryExecNestedTypeFunction<int32_t, DST_TYPE, CastBetweenDecimal>;
            break;
        case PhysicalTypeID::INT64:
            func =
                ScalarFunction::UnaryExecNestedTypeFunction<int64_t, DST_TYPE, CastBetweenDecimal>;
            break;
        case PhysicalTypeID::INT128:
            func =
                ScalarFunction::UnaryExecNestedTypeFunction<int128_t, DST_TYPE, CastBetweenDecimal>;
            break;
        default:
            KU_UNREACHABLE;
        }
    } break;
    default:
        throw ConversionException{stringFormat("Unsupported casting function from {} to {}.",
            sourceType.toString(), targetType.toString())};
    }
    return std::make_unique<ScalarFunction>(functionName,
        std::vector<LogicalTypeID>{sourceType.getLogicalTypeID()}, targetType.getLogicalTypeID(),
        func);
}

template<typename DST_TYPE, typename OP, typename EXECUTOR = UnaryFunctionExecutor>
static std::unique_ptr<ScalarFunction> bindCastToNumericFunction(const std::string& functionName,
    const LogicalType& sourceType, const LogicalType& targetType) {
    scalar_func_exec_t func;
    switch (sourceType.getLogicalTypeID()) {
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
        func = ScalarFunction::UnaryExecFunction<float, DST_TYPE, OP, EXECUTOR>;
    } break;
    case LogicalTypeID::DOUBLE: {
        func = ScalarFunction::UnaryExecFunction<double, DST_TYPE, OP, EXECUTOR>;
    } break;
    case LogicalTypeID::DECIMAL: {
        switch (sourceType.getPhysicalType()) {
        // note: this cannot handle decimal -> decimal casting.
        case PhysicalTypeID::INT16:
            func = ScalarFunction::UnaryExecNestedTypeFunction<int16_t, DST_TYPE, CastDecimalTo>;
            break;
        case PhysicalTypeID::INT32:
            func = ScalarFunction::UnaryExecNestedTypeFunction<int32_t, DST_TYPE, CastDecimalTo>;
            break;
        case PhysicalTypeID::INT64:
            func = ScalarFunction::UnaryExecNestedTypeFunction<int64_t, DST_TYPE, CastDecimalTo>;
            break;
        case PhysicalTypeID::INT128:
            func = ScalarFunction::UnaryExecNestedTypeFunction<int128_t, DST_TYPE, CastDecimalTo>;
            break;
        default:
            KU_UNREACHABLE;
        }
    } break;
    default:
        throw ConversionException{stringFormat("Unsupported casting function from {} to {}.",
            sourceType.toString(), targetType.toString())};
    }
    return std::make_unique<ScalarFunction>(functionName,
        std::vector<LogicalTypeID>{sourceType.getLogicalTypeID()}, targetType.getLogicalTypeID(),
        func);
}

template<typename EXECUTOR = UnaryFunctionExecutor>
static std::unique_ptr<ScalarFunction> bindCastBetweenNested(const std::string& functionName,
    const LogicalType& sourceType, const LogicalType& targetType) {
    switch (sourceType.getLogicalTypeID()) {
    case LogicalTypeID::LIST:
    case LogicalTypeID::MAP:
    case LogicalTypeID::STRUCT:
    case LogicalTypeID::ARRAY: {
        // todo: compile time checking of nested types
        if (CastArrayHelper::checkCompatibleNestedTypes(sourceType.getLogicalTypeID(),
                targetType.getLogicalTypeID())) {
            return std::make_unique<ScalarFunction>(functionName,
                std::vector<LogicalTypeID>{sourceType.getLogicalTypeID()},
                targetType.getLogicalTypeID(), nestedTypesCastExecFunction);
        }
        [[fallthrough]];
    }
    default:
        throw ConversionException{stringFormat("Unsupported casting function from {} to {}.",
            LogicalTypeUtils::toString(sourceType.getLogicalTypeID()),
            LogicalTypeUtils::toString(targetType.getLogicalTypeID()))};
    }
}

template<typename EXECUTOR = UnaryFunctionExecutor, typename DST_TYPE>
static std::unique_ptr<ScalarFunction> bindCastToDateFunction(const std::string& functionName,
    const LogicalType& sourceType, const LogicalType& dstType) {
    scalar_func_exec_t func;
    switch (sourceType.getLogicalTypeID()) {
    case LogicalTypeID::TIMESTAMP_MS:
        func = ScalarFunction::UnaryExecFunction<timestamp_ms_t, DST_TYPE, CastToDate, EXECUTOR>;
        break;
    case LogicalTypeID::TIMESTAMP_NS:
        func = ScalarFunction::UnaryExecFunction<timestamp_ns_t, DST_TYPE, CastToDate, EXECUTOR>;
        break;
    case LogicalTypeID::TIMESTAMP_SEC:
        func = ScalarFunction::UnaryExecFunction<timestamp_sec_t, DST_TYPE, CastToDate, EXECUTOR>;
        break;
    case LogicalTypeID::TIMESTAMP_TZ:
    case LogicalTypeID::TIMESTAMP:
        func = ScalarFunction::UnaryExecFunction<timestamp_t, DST_TYPE, CastToDate, EXECUTOR>;
        break;
    // LCOV_EXCL_START
    default:
        throw ConversionException{stringFormat("Unsupported casting function from {} to {}.",
            sourceType.toString(), dstType.toString())};
        // LCOV_EXCL_END
    }
    return std::make_unique<ScalarFunction>(functionName,
        std::vector<LogicalTypeID>{sourceType.getLogicalTypeID()}, LogicalTypeID::DATE, func);
}

template<typename EXECUTOR = UnaryFunctionExecutor, typename DST_TYPE>
static std::unique_ptr<ScalarFunction> bindCastToTimestampFunction(const std::string& functionName,
    const LogicalType& sourceType, const LogicalType& dstType) {
    scalar_func_exec_t func;
    switch (sourceType.getLogicalTypeID()) {
    case LogicalTypeID::DATE: {
        func = ScalarFunction::UnaryExecFunction<date_t, DST_TYPE, CastDateToTimestamp, EXECUTOR>;
    } break;
    case LogicalTypeID::TIMESTAMP_MS: {
        func = ScalarFunction::UnaryExecFunction<timestamp_ms_t, DST_TYPE, CastBetweenTimestamp,
            EXECUTOR>;
    } break;
    case LogicalTypeID::TIMESTAMP_NS: {
        func = ScalarFunction::UnaryExecFunction<timestamp_ns_t, DST_TYPE, CastBetweenTimestamp,
            EXECUTOR>;
    } break;
    case LogicalTypeID::TIMESTAMP_SEC: {
        func = ScalarFunction::UnaryExecFunction<timestamp_sec_t, DST_TYPE, CastBetweenTimestamp,
            EXECUTOR>;
    } break;
    case LogicalTypeID::TIMESTAMP_TZ:
    case LogicalTypeID::TIMESTAMP: {
        func = ScalarFunction::UnaryExecFunction<timestamp_t, DST_TYPE, CastBetweenTimestamp,
            EXECUTOR>;
    } break;
    default:
        throw ConversionException{stringFormat("Unsupported casting function from {} to {}.",
            sourceType.toString(), dstType.toString())};
    }
    return std::make_unique<ScalarFunction>(functionName,
        std::vector<LogicalTypeID>{sourceType.getLogicalTypeID()}, LogicalTypeID::TIMESTAMP, func);
}

template<typename DST_TYPE>
static std::unique_ptr<ScalarFunction> bindCastBetweenDecimalFunction(
    const std::string& functionName, const LogicalType& sourceType) {
    scalar_func_exec_t func;
    switch (sourceType.getPhysicalType()) {
    case PhysicalTypeID::INT16:
        func = ScalarFunction::UnaryExecNestedTypeFunction<int16_t, DST_TYPE, CastBetweenDecimal>;
        break;
    case PhysicalTypeID::INT32:
        func = ScalarFunction::UnaryExecNestedTypeFunction<int32_t, DST_TYPE, CastBetweenDecimal>;
        break;
    case PhysicalTypeID::INT64:
        func = ScalarFunction::UnaryExecNestedTypeFunction<int64_t, DST_TYPE, CastBetweenDecimal>;
        break;
    case PhysicalTypeID::INT128:
        func = ScalarFunction::UnaryExecNestedTypeFunction<int128_t, DST_TYPE, CastBetweenDecimal>;
        break;
    default:
        KU_UNREACHABLE;
    }
    return std::make_unique<ScalarFunction>(functionName,
        std::vector<LogicalTypeID>{LogicalTypeID::DECIMAL}, LogicalTypeID::DECIMAL, func);
}

template<typename EXECUTOR>
std::unique_ptr<ScalarFunction> CastFunction::bindCastFunction(const std::string& functionName,
    const LogicalType& sourceType, const LogicalType& targetType) {
    auto sourceTypeID = sourceType.getLogicalTypeID();
    auto targetTypeID = targetType.getLogicalTypeID();
    if (sourceTypeID == LogicalTypeID::STRING) {
        return bindCastFromStringFunction<EXECUTOR>(functionName, targetType);
    }
    if (sourceTypeID == LogicalTypeID::RDF_VARIANT) {
        return bindCastFromRdfVariantFunction(functionName, targetType);
    }
    switch (targetTypeID) {
    case LogicalTypeID::STRING: {
        return bindCastToStringFunction<EXECUTOR>(functionName, sourceType);
    }
    case LogicalTypeID::RDF_VARIANT: {
        return bindCastToRdfVariantFunction(functionName, sourceType);
    }
    case LogicalTypeID::DOUBLE: {
        return bindCastToNumericFunction<double, CastToDouble, EXECUTOR>(functionName, sourceType,
            targetType);
    }
    case LogicalTypeID::FLOAT: {
        return bindCastToNumericFunction<float, CastToFloat, EXECUTOR>(functionName, sourceType,
            targetType);
    }
    case LogicalTypeID::DECIMAL: {
        switch (targetType.getPhysicalType()) {
        case PhysicalTypeID::INT16:
            return bindCastToDecimalFunction<int16_t>(functionName, sourceType, targetType);
        case PhysicalTypeID::INT32:
            return bindCastToDecimalFunction<int32_t>(functionName, sourceType, targetType);
        case PhysicalTypeID::INT64:
            return bindCastToDecimalFunction<int64_t>(functionName, sourceType, targetType);
        case PhysicalTypeID::INT128:
            return bindCastToDecimalFunction<int128_t>(functionName, sourceType, targetType);
        default:
            KU_UNREACHABLE;
        }
    }
    case LogicalTypeID::INT128: {
        return bindCastToNumericFunction<int128_t, CastToInt128, EXECUTOR>(functionName, sourceType,
            targetType);
    }
    case LogicalTypeID::SERIAL: {
        return bindCastToNumericFunction<int64_t, CastToSerial, EXECUTOR>(functionName, sourceType,
            targetType);
    }
    case LogicalTypeID::INT64: {
        return bindCastToNumericFunction<int64_t, CastToInt64, EXECUTOR>(functionName, sourceType,
            targetType);
    }
    case LogicalTypeID::INT32: {
        return bindCastToNumericFunction<int32_t, CastToInt32, EXECUTOR>(functionName, sourceType,
            targetType);
    }
    case LogicalTypeID::INT16: {
        return bindCastToNumericFunction<int16_t, CastToInt16, EXECUTOR>(functionName, sourceType,
            targetType);
    }
    case LogicalTypeID::INT8: {
        return bindCastToNumericFunction<int8_t, CastToInt8, EXECUTOR>(functionName, sourceType,
            targetType);
    }
    case LogicalTypeID::UINT64: {
        return bindCastToNumericFunction<uint64_t, CastToUInt64, EXECUTOR>(functionName, sourceType,
            targetType);
    }
    case LogicalTypeID::UINT32: {
        return bindCastToNumericFunction<uint32_t, CastToUInt32, EXECUTOR>(functionName, sourceType,
            targetType);
    }
    case LogicalTypeID::UINT16: {
        return bindCastToNumericFunction<uint16_t, CastToUInt16, EXECUTOR>(functionName, sourceType,
            targetType);
    }
    case LogicalTypeID::UINT8: {
        return bindCastToNumericFunction<uint8_t, CastToUInt8, EXECUTOR>(functionName, sourceType,
            targetType);
    }
    case LogicalTypeID::DATE: {
        return bindCastToDateFunction<EXECUTOR, date_t>(functionName, sourceType, targetType);
    }
    case LogicalTypeID::TIMESTAMP_NS: {
        return bindCastToTimestampFunction<EXECUTOR, timestamp_ns_t>(functionName, sourceType,
            targetType);
    }
    case LogicalTypeID::TIMESTAMP_MS: {
        return bindCastToTimestampFunction<EXECUTOR, timestamp_ms_t>(functionName, sourceType,
            targetType);
    }
    case LogicalTypeID::TIMESTAMP_SEC: {
        return bindCastToTimestampFunction<EXECUTOR, timestamp_sec_t>(functionName, sourceType,
            targetType);
    }
    case LogicalTypeID::TIMESTAMP_TZ:
    case LogicalTypeID::TIMESTAMP: {
        return bindCastToTimestampFunction<EXECUTOR, timestamp_t>(functionName, sourceType,
            targetType);
    }
    case LogicalTypeID::LIST:
    case LogicalTypeID::ARRAY:
    case LogicalTypeID::MAP:
    case LogicalTypeID::STRUCT: {
        return bindCastBetweenNested<EXECUTOR>(functionName, sourceType, targetType);
    }
    default: {
        throw ConversionException(stringFormat("Unsupported casting function from {} to {}.",
            sourceType.toString(), targetType.toString()));
    }
    }
}

function_set CastToDateFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::STRING(), LogicalType::DATE()));
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::RDF_VARIANT(), LogicalType::DATE()));
    return result;
}

function_set CastToTimestampFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::STRING(), LogicalType::TIMESTAMP()));
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::RDF_VARIANT(), LogicalType::TIMESTAMP()));
    return result;
}

function_set CastToIntervalFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::STRING(), LogicalType::INTERVAL()));
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::RDF_VARIANT(), LogicalType::INTERVAL()));
    return result;
}

static std::unique_ptr<FunctionBindData> toStringBindFunc(
    const binder::expression_vector& arguments, Function*) {
    return FunctionBindData::getSimpleBindData(arguments, LogicalType::STRING());
}

function_set CastToStringFunction::getFunctionSet() {
    function_set result;
    result.reserve(LogicalTypeUtils::getAllValidLogicTypes().size());
    for (auto& type : LogicalTypeUtils::getAllValidLogicTypes()) {
        auto function = CastFunction::bindCastFunction(name, type, LogicalType::STRING());
        function->bindFunc = toStringBindFunc;
        result.push_back(std::move(function));
    }
    return result;
}

function_set CastToBlobFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::STRING(), LogicalType::BLOB()));
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::RDF_VARIANT(), LogicalType::BLOB()));
    return result;
}

function_set CastToUUIDFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::STRING(), LogicalType::UUID()));
    return result;
}

function_set CastToBoolFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::STRING(), LogicalType::BOOL()));
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::RDF_VARIANT(), LogicalType::BOOL()));
    return result;
}

function_set CastToDoubleFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(
            CastFunction::bindCastFunction(name, LogicalType(typeID), LogicalType::DOUBLE()));
    }
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::STRING(), LogicalType::DOUBLE()));
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::RDF_VARIANT(), LogicalType::DOUBLE()));
    return result;
}

function_set CastToFloatFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(
            CastFunction::bindCastFunction(name, LogicalType(typeID), LogicalType::FLOAT()));
    }
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::STRING(), LogicalType::FLOAT()));
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::RDF_VARIANT(), LogicalType::FLOAT()));
    return result;
}

function_set CastToInt128Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(
            CastFunction::bindCastFunction(name, LogicalType(typeID), LogicalType::INT128()));
    }
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::STRING(), LogicalType::INT128()));
    return result;
}

function_set CastToSerialFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(
            CastFunction::bindCastFunction(name, LogicalType(typeID), LogicalType::SERIAL()));
    }
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::STRING(), LogicalType::SERIAL()));
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::RDF_VARIANT(), LogicalType::SERIAL()));
    return result;
}

function_set CastToInt64Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(
            CastFunction::bindCastFunction(name, LogicalType(typeID), LogicalType::INT64()));
    }
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::STRING(), LogicalType::INT64()));
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::RDF_VARIANT(), LogicalType::INT64()));
    return result;
}

function_set CastToInt32Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(
            CastFunction::bindCastFunction(name, LogicalType(typeID), LogicalType::INT32()));
    }
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::STRING(), LogicalType::INT32()));
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::RDF_VARIANT(), LogicalType::INT32()));
    return result;
}

function_set CastToInt16Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(
            CastFunction::bindCastFunction(name, LogicalType(typeID), LogicalType::INT16()));
    }
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::STRING(), LogicalType::INT16()));
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::RDF_VARIANT(), LogicalType::INT16()));
    return result;
}

function_set CastToInt8Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(
            CastFunction::bindCastFunction(name, LogicalType(typeID), LogicalType::INT8()));
    }
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::STRING(), LogicalType::INT8()));
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::RDF_VARIANT(), LogicalType::INT8()));
    return result;
}

function_set CastToUInt64Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(
            CastFunction::bindCastFunction(name, LogicalType(typeID), LogicalType::UINT64()));
    }
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::STRING(), LogicalType::UINT64()));
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::RDF_VARIANT(), LogicalType::UINT64()));
    return result;
}

function_set CastToUInt32Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(
            CastFunction::bindCastFunction(name, LogicalType(typeID), LogicalType::UINT32()));
    }
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::STRING(), LogicalType::UINT32()));
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::RDF_VARIANT(), LogicalType::UINT32()));
    return result;
}

function_set CastToUInt16Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(
            CastFunction::bindCastFunction(name, LogicalType(typeID), LogicalType::UINT16()));
    }
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::STRING(), LogicalType::UINT16()));
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::RDF_VARIANT(), LogicalType::UINT16()));
    return result;
}

function_set CastToUInt8Function::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(
            CastFunction::bindCastFunction(name, LogicalType(typeID), LogicalType::UINT8()));
    }
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::STRING(), LogicalType::UINT8()));
    result.push_back(
        CastFunction::bindCastFunction(name, LogicalType::RDF_VARIANT(), LogicalType::UINT8()));
    return result;
}

// TODO(Xiyang): I think it is better to create a new grammar/syntax for casting operations.
//  E.g. Instead of reusing the function grammar (cast(3, 'string')), i think it is better to
//  provide the user with a new grammar: cast(3 as string) similar to duckdb.
static std::unique_ptr<FunctionBindData> castBindFunc(const binder::expression_vector& arguments,
    Function* function) {
    KU_ASSERT(arguments.size() == 2);
    // Bind target type.
    if (arguments[1]->expressionType != ExpressionType::LITERAL) {
        throw BinderException(stringFormat("Second parameter of CAST function must be a literal."));
    }
    auto literalExpr = arguments[1]->constPtrCast<LiteralExpression>();
    auto targetTypeStr = literalExpr->getValue().getValue<std::string>();
    // TODO(Ziyi): we should pass the clientContext pointer here so the bind function can access
    // the user defined types.
    auto targetType = LogicalType::fromString(targetTypeStr);
    if (targetType == arguments[0]->getDataType()) { // No need to cast.
        return nullptr;
    }
    if (ExpressionUtil::canCastStatically(*arguments[0], targetType)) {
        arguments[0]->cast(targetType);
        return nullptr;
    }
    auto func = ku_dynamic_cast<Function*, ScalarFunction*>(function);
    func->name = "CAST_TO_" + targetTypeStr;
    func->execFunc =
        CastFunction::bindCastFunction(func->name, arguments[0]->getDataType(), targetType)
            ->execFunc;
    return std::make_unique<function::CastFunctionBindData>(targetType.copy());
}

function_set CastAnyFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY, LogicalTypeID::STRING}, LogicalTypeID::ANY,
        nullptr, nullptr, castBindFunc));
    return result;
}

} // namespace function
} // namespace kuzu
