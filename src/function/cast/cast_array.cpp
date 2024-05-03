#include "function/cast/functions/cast_array.h"

#include "common/exception/conversion.h"
#include "common/type_utils.h"

namespace kuzu {
namespace function {

bool CastArrayHelper::checkCompatibleNestedTypes(LogicalTypeID sourceTypeID,
    LogicalTypeID targetTypeID) {
    switch (sourceTypeID) {
    case LogicalTypeID::LIST: {
        if (targetTypeID == LogicalTypeID::ARRAY || targetTypeID == LogicalTypeID::LIST) {
            return true;
        }
    } break;
    case LogicalTypeID::MAP:
    case LogicalTypeID::STRUCT: {
        if (sourceTypeID == targetTypeID) {
            return true;
        }
    } break;
    case LogicalTypeID::ARRAY: {
        if (targetTypeID == LogicalTypeID::LIST || targetTypeID == LogicalTypeID::ARRAY) {
            return true;
        }
    } break;
    default:
        return false;
    }
    return false;
}

bool CastArrayHelper::containsListToArray(const LogicalType& srcType, const LogicalType& dstType) {
    if ((srcType.getLogicalTypeID() == LogicalTypeID::LIST ||
            srcType.getLogicalTypeID() == LogicalTypeID::ARRAY) &&
        dstType.getLogicalTypeID() == LogicalTypeID::ARRAY) {
        return true;
    }

    if (checkCompatibleNestedTypes(srcType.getLogicalTypeID(), dstType.getLogicalTypeID())) {
        switch (srcType.getPhysicalType()) {
        case PhysicalTypeID::LIST: {
            return containsListToArray(ListType::getChildType(srcType),
                ListType::getChildType(dstType));
        }
        case PhysicalTypeID::ARRAY: {
            return containsListToArray(ArrayType::getChildType(srcType),
                ListType::getChildType(dstType));
        }
        case PhysicalTypeID::STRUCT: {
            auto srcFieldTypes = StructType::getFieldTypes(srcType);
            auto dstFieldTypes = StructType::getFieldTypes(dstType);
            if (srcFieldTypes.size() != dstFieldTypes.size()) {
                throw ConversionException{
                    stringFormat("Unsupported casting function from {} to {}.", srcType.toString(),
                        dstType.toString())};
            }

            for (auto i = 0u; i < srcFieldTypes.size(); i++) {
                if (containsListToArray(srcFieldTypes[i], dstFieldTypes[i])) {
                    return true;
                }
            }
        } break;
        default:
            return false;
        }
    }
    return false;
}

void CastArrayHelper::validateListEntry(ValueVector* inputVector, const LogicalType& resultType,
    uint64_t pos) {
    if (inputVector->isNull(pos)) {
        return;
    }
    auto inputType = inputVector->dataType;

    switch (resultType.getPhysicalType()) {
    case PhysicalTypeID::ARRAY: {
        if (inputType.getPhysicalType() == PhysicalTypeID::LIST) {
            auto listEntry = inputVector->getValue<list_entry_t>(pos);
            if (listEntry.size != ArrayType::getNumElements(resultType)) {
                throw ConversionException{
                    stringFormat("Unsupported casting LIST with incorrect list entry to ARRAY. "
                                 "Expected: {}, Actual: {}.",
                        ArrayType::getNumElements(resultType),
                        inputVector->getValue<list_entry_t>(pos).size)};
            }
            auto inputChildVector = ListVector::getDataVector(inputVector);
            for (auto i = listEntry.offset; i < listEntry.offset + listEntry.size; i++) {
                validateListEntry(inputChildVector, ArrayType::getChildType(resultType), i);
            }
        } else if (inputType.getPhysicalType() == PhysicalTypeID::ARRAY) {
            if (ArrayType::getNumElements(inputType) != ArrayType::getNumElements(resultType)) {
                throw ConversionException(
                    stringFormat("Unsupported casting function from {} to {}.",
                        inputType.toString(), resultType.toString()));
            }
            auto listEntry = inputVector->getValue<list_entry_t>(pos);
            auto inputChildVector = ListVector::getDataVector(inputVector);
            for (auto i = listEntry.offset; i < listEntry.offset + listEntry.size; i++) {
                validateListEntry(inputChildVector, ArrayType::getChildType(resultType), i);
            }
        }
    } break;
    case PhysicalTypeID::LIST: {
        if (inputType.getPhysicalType() == PhysicalTypeID::LIST ||
            inputType.getPhysicalType() == PhysicalTypeID::ARRAY) {
            auto listEntry = inputVector->getValue<list_entry_t>(pos);
            auto inputChildVector = ListVector::getDataVector(inputVector);
            for (auto i = listEntry.offset; i < listEntry.offset + listEntry.size; i++) {
                validateListEntry(inputChildVector, ListType::getChildType(resultType), i);
            }
        }
    } break;
    case PhysicalTypeID::STRUCT: {
        if (inputType.getPhysicalType() == PhysicalTypeID::STRUCT) {
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

} // namespace function
} // namespace kuzu
