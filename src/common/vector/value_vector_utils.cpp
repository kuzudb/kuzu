#include "src/common/include/vector/value_vector_utils.h"

#include "src/common/include/type_utils.h"
#include "src/common/types/include/value.h"

using namespace graphflow;
using namespace common;

void ValueVectorUtils::addLiteralToStructuredVector(
    ValueVector& resultVector, uint64_t pos, const Literal& literal) {
    switch (literal.dataType) {
    case INT64: {
        ((int64_t*)resultVector.values.get())[pos] = literal.val.int64Val;
    } break;
    case DOUBLE: {
        ((double_t*)resultVector.values.get())[pos] = literal.val.doubleVal;
    } break;
    case BOOL: {
        ((bool*)resultVector.values.get())[pos] = literal.val.booleanVal;
    } break;
    case DATE: {
        ((date_t*)resultVector.values.get())[pos] = literal.val.dateVal;
    } break;
    case TIMESTAMP: {
        ((timestamp_t*)resultVector.values.get())[pos] = literal.val.timestampVal;
    } break;
    case INTERVAL: {
        ((interval_t*)resultVector.values.get())[pos] = literal.val.intervalVal;
    } break;
    case STRING: {
        resultVector.addString(pos, literal.strVal);
    } break;
    default:
        assert(false);
    }
}

void ValueVectorUtils::addLiteralToUnstructuredVector(
    ValueVector& resultVector, uint64_t pos, const Literal& value) {
    assert(resultVector.dataType == UNSTRUCTURED);
    auto& val = ((Value*)resultVector.values.get())[pos];
    val.dataType = value.dataType;
    switch (val.dataType) {
    case INT64: {
        val.val.int64Val = value.val.int64Val;
    } break;
    case DOUBLE: {
        val.val.doubleVal = value.val.doubleVal;
    } break;
    case BOOL: {
        val.val.booleanVal = value.val.booleanVal;
    } break;
    case DATE: {
        val.val.dateVal = value.val.dateVal;
    } break;
    case TIMESTAMP: {
        val.val.timestampVal = value.val.timestampVal;
    } break;
    case INTERVAL: {
        val.val.intervalVal = value.val.intervalVal;
    } break;
    case STRING: {
        TypeUtils::copyString(value.strVal, val.val.strVal, *resultVector.overflowBuffer);
    } break;
    default:
        assert(false);
    }
}

void ValueVectorUtils::addGFStringToUnstructuredVector(
    ValueVector& resultVector, uint64_t pos, const gf_string_t& value) {
    assert(resultVector.dataType == UNSTRUCTURED);
    auto& val = ((Value*)resultVector.values.get())[pos];
    val.dataType = STRING;
    TypeUtils::copyString(value, val.val.strVal, *resultVector.overflowBuffer);
}

void ValueVectorUtils::copyNonNullDataWithSameTypeIntoPos(
    ValueVector& resultVector, uint64_t pos, const uint8_t* srcData) {
    copyNonNullDataWithSameType(resultVector.dataType, srcData,
        resultVector.values.get() + pos * resultVector.getNumBytesPerValue(),
        *resultVector.overflowBuffer);
}

void ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(const ValueVector& srcVector,
    uint64_t pos, uint8_t* dstData, OverflowBuffer& dstOverflowBuffer) {
    copyNonNullDataWithSameType(srcVector.dataType,
        srcVector.values.get() + pos * srcVector.getNumBytesPerValue(), dstData, dstOverflowBuffer);
}

void ValueVectorUtils::copyNonNullDataWithSameType(
    DataType dataType, const uint8_t* srcData, uint8_t* dstData, OverflowBuffer& overflowBuffer) {
    if (dataType == STRING) {
        TypeUtils::copyString(*(gf_string_t*)srcData, *(gf_string_t*)dstData, overflowBuffer);
    } else if (dataType == LIST) {
        TypeUtils::copyList(*(gf_list_t*)srcData, *(gf_list_t*)dstData, overflowBuffer);
    } else {
        // Regardless of whether the dataType is unstructured or a structured non-string type, we
        // first copy over the data in the src to the dst.
        memcpy(dstData, srcData, Types::getDataTypeSize(dataType));
        if (dataType == UNSTRUCTURED) {
            auto unstrValueSrcPtr = (Value*)srcData;
            auto unstrValueDstPtr = (Value*)dstData;
            // If further the dataType is unstructured and string we need to copy over the string
            // overflow if necessary. Recall that an unstructured string has 16 bytes for the string
            // but also stores its data type as an additional byte. That is why we need to copy over
            // the entire data first even though the unstrValueDstPtr->val.strVal.set call below
            // will copy over the 16 bytes for the string.
            if (unstrValueSrcPtr->dataType == STRING) {
                TypeUtils::copyString(
                    unstrValueSrcPtr->val.strVal, unstrValueDstPtr->val.strVal, overflowBuffer);
            }
        }
    }
}
