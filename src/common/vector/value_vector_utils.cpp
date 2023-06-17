#include "common/vector/value_vector_utils.h"

#include "common/null_buffer.h"

using namespace kuzu;
using namespace common;

void ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(const ValueVector& srcVector,
    uint64_t pos, uint8_t* dstData, InMemOverflowBuffer& dstOverflowBuffer) {
    switch (srcVector.dataType.getPhysicalType()) {
    case PhysicalTypeID::STRUCT: {
        // The storage structure of STRUCT type in factorizedTable is:
        // [NULLBYTES, FIELD1, FIELD2, ...]
        auto structFields = StructVector::getFieldVectors(&srcVector);
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
            structValues += LogicalTypeUtils::getRowLayoutSize(structField->dataType);
        }
    } break;
    case PhysicalTypeID::VAR_LIST: {
        auto srcListEntry = srcVector.getValue<list_entry_t>(pos);
        auto srcListDataVector = common::ListVector::getDataVector(&srcVector);
        ku_list_t dstList;
        dstList.size = srcListEntry.size;
        auto dstListOverflowSize =
            LogicalTypeUtils::getRowLayoutSize(srcListDataVector->dataType) * dstList.size +
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
            dstListValues += LogicalTypeUtils::getRowLayoutSize(srcListDataVector->dataType);
        }
        memcpy(dstData, &dstList, sizeof(dstList));
    } break;
    case PhysicalTypeID::STRING: {
        auto& srcStr = srcVector.getValue<ku_string_t>(pos);
        auto& dstStr = *(ku_string_t*)dstData;
        if (ku_string_t::isShortString(srcStr.len)) {
            dstStr.setShortString(srcStr);
        } else {
            dstStr.overflowPtr =
                reinterpret_cast<uint64_t>(dstOverflowBuffer.allocateSpace(srcStr.len));
            dstStr.setLongString(srcStr);
        }
    } break;
    default: {
        auto dataTypeSize = LogicalTypeUtils::getRowLayoutSize(srcVector.dataType);
        memcpy(dstData, srcVector.getData() + pos * dataTypeSize, dataTypeSize);
    }
    }
}
