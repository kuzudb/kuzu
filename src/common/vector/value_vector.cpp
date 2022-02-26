#include "src/common/include/vector/value_vector.h"

#include "src/common/include/operations/comparison_operations.h"

using namespace graphflow::common::operation;

namespace graphflow {
namespace common {

NullMask::NullMask() : mayContainNulls{false} {
    mask = make_unique<bool[]>(DEFAULT_VECTOR_CAPACITY);
    fill_n(mask.get(), DEFAULT_VECTOR_CAPACITY, false /* not null */);
}

ValueVector::ValueVector(MemoryManager* memoryManager, DataType dataType)
    : dataType{dataType}, memoryManager{memoryManager} {
    assert(memoryManager != nullptr);
    // TODO: Once MemoryManager's allocateOSBackedBlock is removed, this memory should be obtained
    // directly through the OS. If that's , make sure to delete the memory during deconstruction.
    bufferValues = memoryManager->allocateOSBackedBlock(
        TypeUtils::getDataTypeSize(dataType) * DEFAULT_VECTOR_CAPACITY);
    values = bufferValues->data;
    if (needOverflowBuffer()) {
        overflowBuffer = make_unique<OverflowBuffer>(memoryManager);
    }
    nullMask = make_shared<NullMask>();
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

void ValueVector::allocateStringOverflowSpaceIfNecessary(gf_string_t& result, uint64_t len) const {
    assert(dataType == STRING || dataType == UNSTRUCTURED);
    overflowBuffer->allocateLargeStringIfNecessary(result, len);
}

void ValueVector::copyNonNullDataWithSameTypeIntoPos(uint64_t pos, uint8_t* srcData) {
    copyNonNullDataWithSameType(srcData, values + pos * getNumBytesPerValue(), *overflowBuffer);
}

void ValueVector::copyNonNullDataWithSameTypeOutFromPos(
    uint64_t pos, uint8_t* dstData, OverflowBuffer& dstOverflowBuffer) const {
    copyNonNullDataWithSameType(values + pos * getNumBytesPerValue(), dstData, dstOverflowBuffer);
}

void ValueVector::copyNonNullDataWithSameType(
    const uint8_t* srcData, uint8_t* dstData, OverflowBuffer& overflowBuffer) const {
    if (dataType == STRING) {
        auto gfStrSrcPtr = (gf_string_t*)srcData;
        auto gfStrDstPtr = (gf_string_t*)dstData;
        overflowBuffer.allocateLargeStringIfNecessary(*gfStrDstPtr, gfStrSrcPtr->len);
        gfStrDstPtr->set(*gfStrSrcPtr);
    } else if (dataType == LIST) {
        auto gfListSrcPtr = (gf_list_t*)srcData;
        auto gfListDstPtr = (gf_list_t*)dstData;
        gfListDstPtr->childType = gfListSrcPtr->childType;
        gfListDstPtr->capacity = gfListSrcPtr->capacity;
        gfListDstPtr->size = gfListSrcPtr->size;
        overflowBuffer.allocateList(*gfListDstPtr);
        gfListDstPtr->copyOverflow(*gfListSrcPtr);
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
                overflowBuffer.allocateLargeStringIfNecessary(
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

void ValueVector::addLiteralToStructuredVector(uint64_t pos, const Literal& literal) const {
    switch (literal.dataType) {
    case INT64: {
        ((int64_t*)values)[pos] = literal.val.int64Val;
    } break;
    case DOUBLE: {
        ((double_t*)values)[pos] = literal.val.doubleVal;
    } break;
    case BOOL: {
        ((bool*)values)[pos] = literal.val.booleanVal;
    } break;
    case DATE: {
        ((date_t*)values)[pos] = literal.val.dateVal;
    } break;
    case TIMESTAMP: {
        ((timestamp_t*)values)[pos] = literal.val.timestampVal;
    } break;
    case INTERVAL: {
        ((interval_t*)values)[pos] = literal.val.intervalVal;
    } break;
    case STRING: {
        addString(pos, literal.strVal);
    } break;
    default:
        assert(false);
    }
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
