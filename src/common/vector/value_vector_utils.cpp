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
            srcData += childVector->getNumBytesPerValue();
        }
    } break;
    case VAR_LIST: {
        copyKuListToVector(resultVector, pos, *reinterpret_cast<const ku_list_t*>(srcData));
    } break;
    default: {
        copyNonNullDataWithSameType(resultVector.dataType, srcData,
            resultVector.getData() + pos * resultVector.getNumBytesPerValue(),
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
            dstData += childVector->getNumBytesPerValue();
        }
    } break;
    case VAR_LIST: {
        auto kuList = ValueVectorUtils::convertListEntryToKuList(srcVector, pos, dstOverflowBuffer);
        memcpy(dstData, &kuList, sizeof(kuList));

    } break;
    default: {
        copyNonNullDataWithSameType(srcVector.dataType,
            srcVector.getData() + pos * srcVector.getNumBytesPerValue(), dstData,
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
    assert(dataType.typeID != STRUCT);
    if (dataType.typeID == STRING) {
        InMemOverflowBufferUtils::copyString(
            *(ku_string_t*)srcData, *(ku_string_t*)dstData, inMemOverflowBuffer);
    } else {
        memcpy(dstData, srcData, Types::getDataTypeSize(dataType));
    }
}

ku_list_t ValueVectorUtils::convertListEntryToKuList(
    const ValueVector& srcVector, uint64_t pos, InMemOverflowBuffer& dstOverflowBuffer) {
    auto listEntry = srcVector.getValue<list_entry_t>(pos);
    auto listValues = ListVector::getListValues(&srcVector, listEntry);
    ku_list_t dstList;
    dstList.size = listEntry.size;
    InMemOverflowBufferUtils::allocateSpaceForList(dstList,
        Types::getDataTypeSize(*srcVector.dataType.getChildType()) * dstList.size,
        dstOverflowBuffer);
    auto srcDataVector = ListVector::getDataVector(&srcVector);
    if (srcDataVector->dataType.typeID == VAR_LIST) {
        for (auto i = 0u; i < dstList.size; i++) {
            auto kuList =
                convertListEntryToKuList(*srcDataVector, listEntry.offset + i, dstOverflowBuffer);
            (reinterpret_cast<ku_list_t*>(dstList.overflowPtr))[i] = kuList;
        }
    } else {
        memcpy(reinterpret_cast<uint8_t*>(dstList.overflowPtr), listValues,
            srcDataVector->getNumBytesPerValue() * listEntry.size);
        if (srcDataVector->dataType.typeID == STRING) {
            for (auto i = 0u; i < dstList.size; i++) {
                InMemOverflowBufferUtils::copyString(
                    (reinterpret_cast<ku_string_t*>(listValues))[i],
                    (reinterpret_cast<ku_string_t*>(dstList.overflowPtr))[i], dstOverflowBuffer);
            }
        }
    }
    return dstList;
}

void ValueVectorUtils::copyKuListToVector(
    ValueVector& dstVector, uint64_t pos, const ku_list_t& srcList) {
    auto srcListValues = reinterpret_cast<uint8_t*>(srcList.overflowPtr);
    auto dstListEntry = ListVector::addList(&dstVector, srcList.size);
    dstVector.setValue<list_entry_t>(pos, dstListEntry);
    if (dstVector.dataType.getChildType()->typeID == VAR_LIST) {
        for (auto i = 0u; i < srcList.size; i++) {
            ValueVectorUtils::copyKuListToVector(*ListVector::getDataVector(&dstVector),
                dstListEntry.offset + i, reinterpret_cast<ku_list_t*>(srcList.overflowPtr)[i]);
        }
    } else {
        auto dstDataVector = ListVector::getDataVector(&dstVector);
        auto dstListValues = ListVector::getListValues(&dstVector, dstListEntry);
        memcpy(dstListValues, srcListValues, srcList.size * dstDataVector->getNumBytesPerValue());
        if (dstDataVector->dataType.getTypeID() == STRING) {
            for (auto i = 0u; i < srcList.size; i++) {
                InMemOverflowBufferUtils::copyString(
                    (reinterpret_cast<ku_string_t*>(srcListValues))[i],
                    (reinterpret_cast<ku_string_t*>(dstListValues))[i],
                    *StringVector::getInMemOverflowBuffer(dstDataVector));
            }
        }
    }
}
