#pragma once

#include <cassert>

#include "src/common/include/data_chunk/data_chunk_state.h"
#include "src/common/include/null_mask.h"
#include "src/common/include/overflow_buffer.h"
#include "src/common/types/include/literal.h"

namespace graphflow {
namespace common {

//! A Vector represents values of the same data type.
//! The capacity of a ValueVector is either 1 (sequence) or DEFAULT_VECTOR_CAPACITY.
class ValueVector {

public:
    ValueVector(MemoryManager* memoryManager, DataType dataType);
    ValueVector(MemoryManager* memoryManager, DataTypeID dataTypeID)
        : ValueVector(memoryManager, DataType(dataTypeID)) {
        assert(dataTypeID != LIST);
    }

    ~ValueVector() = default;

    void addString(uint64_t pos, string value) const;
    void addString(uint64_t pos, char* value, uint64_t len) const;

    inline void setAllNull() { nullMask->setAllNull(); }

    inline void setAllNonNull() { nullMask->setAllNonNull(); }

    inline void setMayContainNulls() { nullMask->setMayContainNulls(); }
    // Note that if this function returns true, there are no null. However if it returns false, it
    // doesn't mean there are nulls, i.e., there may or may not be nulls.
    inline bool hasNoNullsGuarantee() {
        // This function should not be used for flat values. For flat values, the null value
        // of the value should be checked directly.
        assert(!state->isFlat());
        return nullMask->hasNoNullsGuarantee();
    }

    inline void setRangeNonNull(uint64_t startPos, uint64_t len) {
        for (auto i = 0u; i < len; ++i) {
            setNull(startPos + i, false);
        }
    }

    inline uint64_t* getNullMaskData() { return nullMask->getData(); }

    inline void setNull(uint64_t pos, bool isNull) { nullMask->setNull(pos, isNull); }

    inline uint8_t isNull(uint32_t pos) const { return nullMask->isNull(pos); }

    inline uint32_t getNumBytesPerValue() const { return Types::getDataTypeSize(dataType); }

    inline node_offset_t readNodeOffset(uint64_t pos) const {
        assert(dataType.typeID == NODE_ID);
        return ((nodeID_t*)values)[pos].offset;
    }

    inline void setSequential() { _isSequential = true; }
    inline bool isSequential() const { return _isSequential; }

    inline OverflowBuffer& getOverflowBuffer() const { return *overflowBuffer; }

    inline void resetOverflowBuffer() const {
        if (overflowBuffer) {
            overflowBuffer->resetBuffer();
        }
    }

private:
    inline bool needOverflowBuffer() const {
        return dataType.typeID == STRING || dataType.typeID == LIST ||
               dataType.typeID == UNSTRUCTURED;
    }

public:
    DataType dataType;
    uint8_t* values;
    shared_ptr<DataChunkState> state;

private:
    bool _isSequential = false;
    unique_ptr<OverflowBuffer> overflowBuffer;
    unique_ptr<uint8_t[]> valueBuffer;
    // This is a shared pointer because sometimes ValueVectors may share NullMasks, e.g., the result
    // ValueVectors of unary expressions, point to the nullMasks of operands.
    shared_ptr<NullMask> nullMask;
};

} // namespace common
} // namespace graphflow
