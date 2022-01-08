#include "src/common/include/vector/value_vector.h"

#include "src/common/include/operations/comparison_operations.h"

using namespace graphflow::common::operation;

namespace graphflow {
namespace common {

shared_ptr<NullMask> NullMask::clone() {
    auto newNullMask = make_shared<NullMask>();
    memcpy(newNullMask->mask.get(), mask.get(), DEFAULT_VECTOR_CAPACITY);
    return newNullMask;
}

void ValueVector::readNodeID(uint64_t pos, nodeID_t& nodeID) const {
    assert(dataType == NODE);
    if (isSequence) {
        nodeID = ((nodeID_t*)values)[0];
        nodeID.offset += pos;
    } else {
        nodeID = ((nodeID_t*)values)[pos];
    }
}

bool ValueVector::discardNullNodes() {
    assert(dataType == NODE);
    if (state->isFlat()) {
        return !isNull(state->getPositionOfCurrIdx());
    } else {
        auto selectedPos = 0u;
        if (state->isUnfiltered()) {
            state->resetSelectorToValuePosBuffer();
            for (auto i = 0u; i < state->selectedSize; i++) {
                state->selectedPositions[selectedPos] = i;
                selectedPos += !isNull(i);
            }
        } else {
            for (auto i = 0u; i < state->selectedSize; i++) {
                auto pos = state->selectedPositions[i];
                state->selectedPositions[selectedPos] = i;
                selectedPos += !isNull(pos);
            }
        }
        state->selectedSize = selectedPos;
        return state->selectedSize > 0;
    }
}

// Notice that this clone function only copies values and mask without copying string buffers.
shared_ptr<ValueVector> ValueVector::clone() {
    auto newVector = make_shared<ValueVector>(memoryManager, dataType);
    // Warning: This is a potential bug because below we copy the nullMask of this ValueVector.
    // However, nullmasks of ValueVectors point to null masks of other ValueVectors, e.g., the
    // result ValueVectors of unary expressions, point to the nullMasks of operands. In which case
    // these should not be copied over. newVector->nullMask = nullMask->clone();
    newVector->nullMask = nullMask->clone();
    // We first blindly copy because we are assuming that the caller (which currently is the
    // ResultCollector is using the DataChunkState of this vector, which may have filted positions.
    // So the selected positions can be a subset of the actual values and if the datatype is
    // string or unstructured (which might have strings), we want to fix the overflow pointers
    // of the copied strings only for the selected positions. For non selected positions, it is not
    // also safe to copy them, as they are not valid strings.
    memcpy(newVector->values, values, DEFAULT_VECTOR_CAPACITY * getNumBytesPerValue());
    if (dataType == STRING || dataType == UNSTRUCTURED) {
        for (int i = 0; i < state->selectedSize; ++i) {
            if (!isNull(state->selectedPositions[i])) {
                newVector->copyNonNullDataWithSameTypeIntoPos(state->selectedPositions[i],
                    values + getNumBytesPerValue() * state->selectedPositions[i]);
            }
        }
    }
    return newVector;
}

void ValueVector::allocateStringOverflowSpaceIfNecessary(gf_string_t& result, uint64_t len) const {
    assert(dataType == STRING || dataType == UNSTRUCTURED);
    stringBuffer->allocateLargeStringIfNecessary(result, len);
}

void ValueVector::copyNonNullDataWithSameTypeIntoPos(uint64_t pos, uint8_t* srcData) {
    copyNonNullDataWithSameType(srcData, values + pos * getNumBytesPerValue(), *stringBuffer);
}

void ValueVector::copyNonNullDataWithSameTypeOutFromPos(
    uint64_t pos, uint8_t* dstData, StringBuffer& dstStringBuffer) {
    copyNonNullDataWithSameType(values + pos * getNumBytesPerValue(), dstData, dstStringBuffer);
}

void ValueVector::copyNonNullDataWithSameType(
    const uint8_t* srcData, uint8_t* dstData, StringBuffer& stringBuffer) const {
    if (dataType == STRING) {
        auto gfStrSrcPtr = (gf_string_t*)srcData;
        auto gfStrDstPtr = (gf_string_t*)dstData;
        stringBuffer.allocateLargeStringIfNecessary(*gfStrDstPtr, gfStrSrcPtr->len);
        gfStrDstPtr->set(*gfStrSrcPtr);
    } else {
        // Regardless of whether the dataType is unstructured or a structured non-string type, we
        // first copy over the data in the src to the dst.
        memcpy(dstData, srcData, getNumBytesPerValue());
        if (dataType == UNSTRUCTURED) {
            auto unstrValueSrcPtr = (Value*)srcData;
            auto unstrValueDstPtr = (Value*)dstData;
            // If further the dataType is unstructured and string we need to copy over the string
            // overflow if necessary. Recall that an unstructured string has 16 bytes for the string
            // but also stores its data type as an additional byte. That is why we need to copy over
            // the entire data first even though the unstrValueDstPtr->val.strVal.set call below
            // will copy over the 16 bytes for the string.
            if (unstrValueSrcPtr->dataType == STRING) {
                stringBuffer.allocateLargeStringIfNecessary(
                    unstrValueDstPtr->val.strVal, unstrValueSrcPtr->val.strVal.len);
                unstrValueDstPtr->val.strVal.set(unstrValueSrcPtr->val.strVal);
            }
        }
    }
}

void ValueVector::addString(uint64_t pos, char* value, uint64_t len) const {
    assert(dataType == STRING);
    auto vectorData = (gf_string_t*)values;
    auto& result = vectorData[pos];
    allocateStringOverflowSpaceIfNecessary(result, len);
    result.set(value, len);
}

void ValueVector::addString(uint64_t pos, string value) const {
    addString(pos, value.data(), value.length());
}

void ValueVector::addLiteralToUnstructuredVector(const uint64_t pos, const Literal& value) const {
    assert(dataType == UNSTRUCTURED);
    auto& val = ((Value*)values)[pos];
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
        allocateStringOverflowSpaceIfNecessary(val.val.strVal, value.strVal.length());
        val.val.strVal.set(value.strVal);
    } break;
    default:
        assert(false);
    }
}

void ValueVector::addGFStringToUnstructuredVector(
    const uint64_t pos, const gf_string_t& value) const {
    assert(dataType == UNSTRUCTURED);
    auto& val = ((Value*)values)[pos];
    val.dataType = STRING;
    allocateStringOverflowSpaceIfNecessary(val.val.strVal, value.len);
    val.val.strVal.set(value);
}

} // namespace common
} // namespace graphflow
