#include "common/vector/value_vector_utils.h"

#include "common/in_mem_overflow_buffer_utils.h"

using namespace kuzu;
using namespace common;

void ValueVectorUtils::copyNonNullDataWithSameTypeIntoPos(
    ValueVector& resultVector, uint64_t pos, const uint8_t* srcData) {
    switch (resultVector.dataType.typeID) {
    case STRUCT: {
        for (auto& childVector : StructVector::getChildrenVectors(&resultVector)) {
            copyNonNullDataWithSameTypeIntoPos(*childVector, pos, srcData);
            srcData += Types::getDataTypeSize(childVector->dataType);
        }
    } break;
    case VAR_LIST: {
        auto srcKuList = *(ku_list_t*)srcData;
        auto srcListValues = reinterpret_cast<uint8_t*>(srcKuList.overflowPtr);
        auto dstListEntry = ListVector::addList(&resultVector, srcKuList.size);
        resultVector.setValue<list_entry_t>(pos, dstListEntry);
        auto resultDataVector = common::ListVector::getDataVector(&resultVector);
        for (auto i = 0u; i < srcKuList.size; i++) {
            copyNonNullDataWithSameTypeIntoPos(
                *resultDataVector, dstListEntry.offset + i, srcListValues);
            srcListValues += Types::getDataTypeSize(resultDataVector->dataType);
        }
    } break;
    default: {
        copyNonNullDataWithSameType(resultVector.dataType, srcData,
            resultVector.getData() + pos * Types::getDataTypeSize(resultVector.dataType),
            *StringVector::getInMemOverflowBuffer(&resultVector));
    }
    }
}

void ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(const ValueVector& srcVector,
    uint64_t pos, uint8_t* dstData, InMemOverflowBuffer& dstOverflowBuffer) {
    switch (srcVector.dataType.typeID) {
    case STRUCT: {
        for (auto& childVector : StructVector::getChildrenVectors(&srcVector)) {
            copyNonNullDataWithSameTypeOutFromPos(*childVector, pos, dstData, dstOverflowBuffer);
            dstData += Types::getDataTypeSize(childVector->dataType);
        }
    } break;
    case VAR_LIST: {
        auto srcListEntry = srcVector.getValue<list_entry_t>(pos);
        auto srcListDataVector = common::ListVector::getDataVector(&srcVector);
        ku_list_t dstList;
        dstList.size = srcListEntry.size;
        InMemOverflowBufferUtils::allocateSpaceForList(dstList,
            Types::getDataTypeSize(srcListDataVector->dataType) * dstList.size, dstOverflowBuffer);
        for (auto i = 0u; i < srcListEntry.size; i++) {
            copyNonNullDataWithSameTypeOutFromPos(*srcListDataVector, srcListEntry.offset + i,
                reinterpret_cast<uint8_t*>(dstList.overflowPtr) +
                    i * Types::getDataTypeSize(srcListDataVector->dataType),
                dstOverflowBuffer);
        }
        memcpy(dstData, &dstList, sizeof(dstList));
    } break;
    default: {
        copyNonNullDataWithSameType(srcVector.dataType,
            srcVector.getData() + pos * Types::getDataTypeSize(srcVector.dataType), dstData,
            dstOverflowBuffer);
    }
    }
}

void ValueVectorUtils::copyValue(uint8_t* dstValue, common::ValueVector& dstVector,
    const uint8_t* srcValue, const common::ValueVector& srcVector) {
    switch (srcVector.dataType.typeID) {
    case VAR_LIST: {
        auto srcList = reinterpret_cast<const common::list_entry_t*>(srcValue);
        auto dstList = reinterpret_cast<common::list_entry_t*>(dstValue);
        *dstList = ListVector::addList(&dstVector, srcList->size);
        auto srcValues = ListVector::getListValues(&srcVector, *srcList);
        auto dstValues = ListVector::getListValues(&dstVector, *dstList);
        auto numBytesPerValue = ListVector::getDataVector(&srcVector)->getNumBytesPerValue();
        for (auto i = 0u; i < srcList->size; i++) {
            copyValue(dstValues, *ListVector::getDataVector(&dstVector), srcValues,
                *ListVector::getDataVector(&srcVector));
            srcValues += numBytesPerValue;
            dstValues += numBytesPerValue;
        }
    } break;
    case STRUCT: {
        auto srcFields = common::StructVector::getChildrenVectors(&srcVector);
        auto dstFields = common::StructVector::getChildrenVectors(&dstVector);
        auto srcPos = *(int64_t*)srcValue;
        auto dstPos = *(int64_t*)dstValue;
        for (auto i = 0u; i < srcFields.size(); i++) {
            auto srcField = srcFields[i];
            auto dstField = dstFields[i];
            copyValue(dstField->getData() + dstField->getNumBytesPerValue() * dstPos, *dstField,
                srcField->getData() + srcField->getNumBytesPerValue() * srcPos, *srcField);
        }
    } break;
    case STRING: {
        common::InMemOverflowBufferUtils::copyString(*(common::ku_string_t*)srcValue,
            *(common::ku_string_t*)dstValue, *StringVector::getInMemOverflowBuffer(&dstVector));
    } break;
    default: {
        memcpy(dstValue, srcValue, srcVector.getNumBytesPerValue());
    }
    }
}

void ValueVectorUtils::copyNonNullDataWithSameType(const DataType& dataType, const uint8_t* srcData,
    uint8_t* dstData, InMemOverflowBuffer& inMemOverflowBuffer) {
    if (dataType.typeID == STRING) {
        InMemOverflowBufferUtils::copyString(
            *(ku_string_t*)srcData, *(ku_string_t*)dstData, inMemOverflowBuffer);
    } else {
        memcpy(dstData, srcData, Types::getDataTypeSize(dataType));
    }
}
