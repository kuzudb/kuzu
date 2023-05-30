#include "common/vector/value_vector_utils.h"

#include "common/in_mem_overflow_buffer_utils.h"
#include "common/null_buffer.h"
#include "processor/result/factorized_table.h"

using namespace kuzu;
using namespace common;

void ValueVectorUtils::copyNonNullDataWithSameTypeIntoPos(
    ValueVector& resultVector, uint64_t pos, const uint8_t* srcData) {
    switch (resultVector.dataType.getLogicalTypeID()) {
    case LogicalTypeID::STRUCT: {
        auto structFields = StructVector::getChildrenVectors(&resultVector);
        auto structNullBytes = srcData;
        auto structValues =
            structNullBytes + NullBuffer::getNumBytesForNullValues(structFields.size());
        for (auto i = 0u; i < structFields.size(); i++) {
            auto structField = structFields[i];
            if (NullBuffer::isNull(structNullBytes, i)) {
                structField->setNull(pos, true /* isNull */);
            } else {
                copyNonNullDataWithSameTypeIntoPos(*structField, pos, structValues);
            }
            structValues += processor::FactorizedTable::getDataTypeSize(structField->dataType);
        }
    } break;
    case LogicalTypeID::RECURSIVE_REL:
    case LogicalTypeID::VAR_LIST: {
        auto srcKuList = *(ku_list_t*)srcData;
        auto srcNullBytes = reinterpret_cast<uint8_t*>(srcKuList.overflowPtr);
        auto srcListValues = srcNullBytes + NullBuffer::getNumBytesForNullValues(srcKuList.size);
        auto dstListEntry = ListVector::addList(&resultVector, srcKuList.size);
        resultVector.setValue<list_entry_t>(pos, dstListEntry);
        auto resultDataVector = common::ListVector::getDataVector(&resultVector);
        auto numBytesPerValue =
            processor::FactorizedTable::getDataTypeSize(resultDataVector->dataType);
        for (auto i = 0u; i < srcKuList.size; i++) {
            auto dstListValuePos = dstListEntry.offset + i;
            if (NullBuffer::isNull(srcNullBytes, i)) {
                resultDataVector->setNull(dstListValuePos, true);
            } else {
                copyNonNullDataWithSameTypeIntoPos(
                    *resultDataVector, dstListValuePos, srcListValues);
            }
            srcListValues += numBytesPerValue;
        }
    } break;
    case LogicalTypeID::STRING: {
        auto dstData = resultVector.getData() +
                       pos * processor::FactorizedTable::getDataTypeSize(resultVector.dataType);
        InMemOverflowBufferUtils::copyString(*(ku_string_t*)srcData, *(ku_string_t*)dstData,
            *StringVector::getInMemOverflowBuffer(&resultVector));
    } break;
    default: {
        auto dataTypeSize = processor::FactorizedTable::getDataTypeSize(resultVector.dataType);
        memcpy(resultVector.getData() + pos * dataTypeSize, srcData, dataTypeSize);
    }
    }
}

void ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(const ValueVector& srcVector,
    uint64_t pos, uint8_t* dstData, InMemOverflowBuffer& dstOverflowBuffer) {
    switch (srcVector.dataType.getLogicalTypeID()) {
    case LogicalTypeID::STRUCT: {
        // The storage structure of STRUCT type in factorizedTable is:
        // [NULLBYTES, FIELD1, FIELD2, ...]
        auto structFields = StructVector::getChildrenVectors(&srcVector);
        NullBuffer::initNullBytes(dstData, structFields.size());
        auto structNullBytes = dstData;
        auto structValues =
            structNullBytes + NullBuffer::getNumBytesForNullValues(structFields.size());
        for (auto i = 0u; i < structFields.size(); i++) {
            auto structField = structFields[i];
            if (structField->isNull(pos)) {
                NullBuffer::setNull(structNullBytes, i);
            } else {
                copyNonNullDataWithSameTypeOutFromPos(
                    *structField, pos, structValues, dstOverflowBuffer);
            }
            structValues += processor::FactorizedTable::getDataTypeSize(structField->dataType);
        }
    } break;
    case LogicalTypeID::RECURSIVE_REL:
    case LogicalTypeID::VAR_LIST: {
        auto srcListEntry = srcVector.getValue<list_entry_t>(pos);
        auto srcListDataVector = common::ListVector::getDataVector(&srcVector);
        ku_list_t dstList;
        dstList.size = srcListEntry.size;
        auto dstListOverflowSize =
            processor::FactorizedTable::getDataTypeSize(srcListDataVector->dataType) *
                dstList.size +
            NullBuffer::getNumBytesForNullValues(dstList.size);
        dstList.overflowPtr =
            reinterpret_cast<uint64_t>(dstOverflowBuffer.allocateSpace(dstListOverflowSize));
        auto dstListNullBytes = reinterpret_cast<uint8_t*>(dstList.overflowPtr);
        NullBuffer::initNullBytes(dstListNullBytes, dstList.size);
        auto dstListValues = dstListNullBytes + NullBuffer::getNumBytesForNullValues(dstList.size);
        for (auto i = 0u; i < srcListEntry.size; i++) {
            if (srcListDataVector->isNull(srcListEntry.offset + i)) {
                NullBuffer::setNull(dstListNullBytes, i);
            } else {
                copyNonNullDataWithSameTypeOutFromPos(
                    *srcListDataVector, srcListEntry.offset + i, dstListValues, dstOverflowBuffer);
            }
            dstListValues +=
                processor::FactorizedTable::getDataTypeSize(srcListDataVector->dataType);
        }
        memcpy(dstData, &dstList, sizeof(dstList));
    } break;
    case LogicalTypeID::STRING: {
        auto srcData = srcVector.getData() +
                       pos * processor::FactorizedTable::getDataTypeSize(srcVector.dataType);
        InMemOverflowBufferUtils::copyString(
            *(ku_string_t*)srcData, *(ku_string_t*)dstData, dstOverflowBuffer);
    } break;
    default: {
        auto dataTypeSize = processor::FactorizedTable::getDataTypeSize(srcVector.dataType);
        memcpy(dstData, srcVector.getData() + pos * dataTypeSize, dataTypeSize);
    }
    }
}

void ValueVectorUtils::copyValue(uint8_t* dstValue, common::ValueVector& dstVector,
    const uint8_t* srcValue, const common::ValueVector& srcVector) {
    switch (srcVector.dataType.getLogicalTypeID()) {
    case LogicalTypeID::RECURSIVE_REL:
    case LogicalTypeID::VAR_LIST: {
        auto srcList = reinterpret_cast<const common::list_entry_t*>(srcValue);
        auto dstList = reinterpret_cast<common::list_entry_t*>(dstValue);
        *dstList = ListVector::addList(&dstVector, srcList->size);
        auto srcValues = ListVector::getListValues(&srcVector, *srcList);
        auto srcDataVector = ListVector::getDataVector(&srcVector);
        auto dstValues = ListVector::getListValues(&dstVector, *dstList);
        auto dstDataVector = ListVector::getDataVector(&dstVector);
        auto numBytesPerValue = srcDataVector->getNumBytesPerValue();
        for (auto i = 0u; i < srcList->size; i++) {
            if (srcDataVector->isNull(srcList->offset + i)) {
                dstDataVector->setNull(dstList->offset + i, true);
            } else {
                copyValue(dstValues, *dstDataVector, srcValues, *srcDataVector);
            }
            srcValues += numBytesPerValue;
            dstValues += numBytesPerValue;
        }
    } break;
    case LogicalTypeID::STRUCT: {
        auto srcFields = common::StructVector::getChildrenVectors(&srcVector);
        auto dstFields = common::StructVector::getChildrenVectors(&dstVector);
        auto srcPos = *(int64_t*)srcValue;
        auto dstPos = *(int64_t*)dstValue;
        for (auto i = 0u; i < srcFields.size(); i++) {
            auto srcField = srcFields[i];
            auto dstField = dstFields[i];
            if (srcField->isNull(srcPos)) {
                dstField->setNull(dstPos, true /* isNull */);
            } else {
                copyValue(dstField->getData() + dstField->getNumBytesPerValue() * dstPos, *dstField,
                    srcField->getData() + srcField->getNumBytesPerValue() * srcPos, *srcField);
            }
        }
    } break;
    case LogicalTypeID::STRING: {
        common::InMemOverflowBufferUtils::copyString(*(common::ku_string_t*)srcValue,
            *(common::ku_string_t*)dstValue, *StringVector::getInMemOverflowBuffer(&dstVector));
    } break;
    default: {
        memcpy(dstValue, srcValue, srcVector.getNumBytesPerValue());
    }
    }
}
