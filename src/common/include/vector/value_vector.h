#pragma once

#include <cassert>

#include "src/common/include/data_chunk/data_chunk_state.h"
#include "src/common/include/vector/overflow_buffer.h"
#include "src/common/types/include/literal.h"
#include "src/common/types/include/type_utils.h"

namespace graphflow {
namespace common {

class NullMask {
    friend class ValueVector;

public:
    NullMask();

private:
    unique_ptr<bool[]> mask;
    bool mayContainNulls;
};

//! A Vector represents values of the same data type.
//! The capacity of a ValueVector is either 1 (sequence) or DEFAULT_VECTOR_CAPACITY.
class ValueVector {

public:
    ValueVector(MemoryManager* memoryManager, DataType dataType);

    ~ValueVector() = default;

    // These two functions assume that the given uint8_t* srcData/dstData are  pointing to a data
    // with the same data type as this ValueVector. If this ValueVector is unstructured, then
    // srcData/dstData are pointing to a Value
    void copyNonNullDataWithSameTypeIntoPos(uint64_t pos, uint8_t* srcData);
    void copyNonNullDataWithSameTypeOutFromPos(
        uint64_t pos, uint8_t* dstData, OverflowBuffer& dstOverflowBuffer) const;
    void addString(uint64_t pos, string value) const;
    void addString(uint64_t pos, char* value, uint64_t len) const;
    void addLiteralToStructuredVector(uint64_t pos, const Literal& literal) const;
    void addLiteralToUnstructuredVector(uint64_t pos, const Literal& value) const;
    void addGFStringToUnstructuredVector(uint64_t pos, const gf_string_t& value) const;
    void allocateStringOverflowSpaceIfNecessary(gf_string_t& result, uint64_t len) const;
    void allocateListOverflow(gf_list_t& list) { overflowBuffer->allocateList(list); }

    inline void setAllNull() {
        std::fill(nullMask->mask.get(), nullMask->mask.get() + state->originalSize, true);
        nullMask->mayContainNulls = true;
    }

    // Note that if this function returns true, there are no null. However if it returns false, it
    // doesn't mean there are nulls, i.e., there may or may not be nulls.
    inline bool hasNoNullsGuarantee() {
        // This function should not be used for flat values. For flat values, the null value
        // of the value should be checked directly.
        assert(!state->isFlat());
        return !nullMask->mayContainNulls;
    }

    inline void setRangeNonNull(uint64_t startPos, uint64_t len) {
        for (auto i = 0u; i < len; ++i) {
            setNull(startPos + i, false);
        }
    }

    inline void setNull(uint64_t pos, bool isNull) {
        nullMask->mask[pos] = isNull;
        nullMask->mayContainNulls |= isNull;
    }

    inline uint8_t isNull(uint32_t pos) const { return nullMask->mask[pos]; }

    inline uint64_t getNumBytesPerValue() const { return TypeUtils::getDataTypeSize(dataType); }

    // TODO: move this to adj column extend
    bool discardNullNodes();

    inline node_offset_t readNodeOffset(uint64_t pos) const {
        assert(dataType == NODE);
        return ((nodeID_t*)values)[pos].offset;
    }

    inline void resetOverflowBuffer() {
        if (overflowBuffer) {
            overflowBuffer->resetBuffer();
        }
    }

private:
    inline bool needOverflowBuffer() const {
        return dataType == STRING || dataType == LIST || dataType == UNSTRUCTURED;
    }

    void copyNonNullDataWithSameType(
        const uint8_t* srcData, uint8_t* dstData, OverflowBuffer& overflowBuffer) const;

public:
    DataType dataType;
    uint8_t* values;
    unique_ptr<OverflowBuffer> overflowBuffer;
    shared_ptr<DataChunkState> state;

private:
    MemoryManager* memoryManager;
    unique_ptr<OSBackedMemoryBlock> bufferValues;
    // This is a shared pointer because sometimes ValueVectors may share NullMasks, e.g., the result
    // ValueVectors of unary expressions, point to the nullMasks of operands.
    shared_ptr<NullMask> nullMask;
};

} // namespace common
} // namespace graphflow
