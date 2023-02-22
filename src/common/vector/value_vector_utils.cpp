#include "common/vector/value_vector_utils.h"

#include "common/in_mem_overflow_buffer_utils.h"

using namespace kuzu;
using namespace common;

void ValueVectorUtils::copyNonNullDataWithSameTypeIntoPos(
    ValueVector& resultVector, uint64_t pos, const uint8_t* srcData) {
    copyNonNullDataWithSameType(resultVector.dataType, srcData,
        resultVector.getData() + pos * resultVector.getNumBytesPerValue(),
        resultVector.getOverflowBuffer());
}

void ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(const ValueVector& srcVector,
    uint64_t pos, uint8_t* dstData, InMemOverflowBuffer& dstOverflowBuffer) {
    copyNonNullDataWithSameType(srcVector.dataType,
        srcVector.getData() + pos * srcVector.getNumBytesPerValue(), dstData, dstOverflowBuffer);
}

void ValueVectorUtils::copyNonNullDataWithSameType(const DataType& dataType, const uint8_t* srcData,
    uint8_t* dstData, InMemOverflowBuffer& inMemOverflowBuffer) {
    if (dataType.typeID == STRING) {
        InMemOverflowBufferUtils::copyString(
            *(ku_string_t*)srcData, *(ku_string_t*)dstData, inMemOverflowBuffer);
    } else if (dataType.typeID == VAR_LIST) {
        InMemOverflowBufferUtils::copyListRecursiveIfNested(
            *(ku_list_t*)srcData, *(ku_list_t*)dstData, dataType, inMemOverflowBuffer);
    } else {
        memcpy(dstData, srcData, Types::getDataTypeSize(dataType));
    }
}
