#include "src/common/include/vector/value_vector_utils.h"

#include "src/common/types/include/value.h"

using namespace graphflow;
using namespace common;

void ValueVectorUtils::addLiteralToStructuredVector(
    ValueVector& resultVector, uint64_t pos, const Literal& literal) {
    if (literal.isNull()) {
        resultVector.setNull(pos, true);
        return;
    }
    switch (literal.dataType.typeID) {
    case INT64: {
        ((int64_t*)resultVector.values)[pos] = literal.val.int64Val;
    } break;
    case DOUBLE: {
        ((double_t*)resultVector.values)[pos] = literal.val.doubleVal;
    } break;
    case BOOL: {
        ((bool*)resultVector.values)[pos] = literal.val.booleanVal;
    } break;
    case DATE: {
        ((date_t*)resultVector.values)[pos] = literal.val.dateVal;
    } break;
    case TIMESTAMP: {
        ((timestamp_t*)resultVector.values)[pos] = literal.val.timestampVal;
    } break;
    case INTERVAL: {
        ((interval_t*)resultVector.values)[pos] = literal.val.intervalVal;
    } break;
    case STRING: {
        resultVector.addString(pos, literal.strVal);
    } break;
    default:
        assert(false);
    }
}

void ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(const ValueVector& srcVector,
    uint64_t pos, uint8_t* dstData, OverflowBuffer& dstOverflowBuffer) {
    copyNonNullDataWithSameType(srcVector.dataType,
        srcVector.values + pos * srcVector.getNumBytesPerValue(), dstData, dstOverflowBuffer);
}

void ValueVectorUtils::copyNonNullDataWithSameType(const DataType& dataType, const uint8_t* srcData,
    uint8_t* dstData, OverflowBuffer& overflowBuffer) {
    if (dataType.typeID == STRING) {
        OverflowBufferUtils::copyString(
            *(gf_string_t*)srcData, *(gf_string_t*)dstData, overflowBuffer);
    } else if (dataType.typeID == LIST) {
        OverflowBufferUtils::copyListRecursiveIfNested(
            *(gf_list_t*)srcData, *(gf_list_t*)dstData, dataType, overflowBuffer);
    } else {
        // Regardless of whether the dataType is unstructured or a structured non-string type, we
        // first copy over the data in the src to the dst.
        memcpy(dstData, srcData, Types::getDataTypeSize(dataType));
        if (dataType.typeID == UNSTRUCTURED) {
            auto unstrValueSrcPtr = (Value*)srcData;
            auto unstrValueDstPtr = (Value*)dstData;
            // If further the dataType is unstructured and string we need to copy over the string
            // overflow if necessary. Recall that an unstructured string has 16 bytes for the string
            // but also stores its data type as an additional byte. That is why we need to copy over
            // the entire data first even though the unstrValueDstPtr->val.strVal.set call below
            // will copy over the 16 bytes for the string.
            if (unstrValueSrcPtr->dataType.typeID == STRING) {
                OverflowBufferUtils::copyString(
                    unstrValueSrcPtr->val.strVal, unstrValueDstPtr->val.strVal, overflowBuffer);
            }
        }
    }
}
