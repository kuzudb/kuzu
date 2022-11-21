#pragma once

#include <cassert>

#include "common/data_chunk/data_chunk_state.h"
#include "common/in_mem_overflow_buffer.h"
#include "common/null_mask.h"
#include "common/types/literal.h"

namespace kuzu {
namespace common {

//! A Vector represents values of the same data type.
//! The capacity of a ValueVector is either 1 (sequence) or DEFAULT_VECTOR_CAPACITY.
class ValueVector {

public:
    explicit ValueVector(DataType dataType, MemoryManager* memoryManager = nullptr);
    explicit ValueVector(DataTypeID dataTypeID, MemoryManager* memoryManager = nullptr)
        : ValueVector(DataType(dataTypeID), memoryManager) {
        assert(dataTypeID != LIST);
    }

    ~ValueVector() = default;

    inline void setState(shared_ptr<DataChunkState> state_) { state = std::move(state_); }

    inline void setAllNull() { nullMask->setAllNull(); }
    inline void setAllNonNull() { nullMask->setAllNonNull(); }
    inline void setMayContainNulls() { nullMask->setMayContainNulls(); }
    // Note that if this function returns true, there are no null. However, if it returns false, it
    // doesn't mean there are nulls, i.e., there may or may not be nulls.
    inline bool hasNoNullsGuarantee() const {
        // This function should not be used for flat values. For flat values, the null value
        // of the value should be checked directly.
        assert(!state->isFlat());
        return nullMask->hasNoNullsGuarantee();
    }
    inline void setRangeNonNull(uint32_t startPos, uint32_t len) {
        for (auto i = 0u; i < len; ++i) {
            setNull(startPos + i, false);
        }
    }
    inline uint64_t* getNullMaskData() { return nullMask->getData(); }
    inline void setNull(uint32_t pos, bool isNull) { nullMask->setNull(pos, isNull); }
    inline uint8_t isNull(uint32_t pos) const { return nullMask->isNull(pos); }

    inline uint32_t getNumBytesPerValue() const { return numBytesPerValue; }

    template<typename T>
    inline T getValue(uint32_t pos) const {
        return ((T*)valueBuffer.get())[pos];
    }
    template<typename T>
    void setValue(uint32_t pos, T val);

    inline uint8_t* getData() const { return valueBuffer.get(); }

    inline node_offset_t readNodeOffset(uint32_t pos) const {
        assert(dataType.typeID == NODE_ID);
        return getValue<nodeID_t>(pos).offset;
    }

    inline void setSequential() { _isSequential = true; }
    inline bool isSequential() const { return _isSequential; }

    inline InMemOverflowBuffer& getOverflowBuffer() const { return *inMemOverflowBuffer; }
    inline void resetOverflowBuffer() const {
        if (inMemOverflowBuffer) {
            inMemOverflowBuffer->resetBuffer();
        }
    }

private:
    inline bool needOverflowBuffer() const {
        return dataType.typeID == STRING || dataType.typeID == LIST;
    }

    void addString(uint32_t pos, char* value, uint64_t len) const;

public:
    DataType dataType;
    shared_ptr<DataChunkState> state;

private:
    bool _isSequential = false;
    unique_ptr<InMemOverflowBuffer> inMemOverflowBuffer;
    unique_ptr<uint8_t[]> valueBuffer;
    unique_ptr<NullMask> nullMask;
    uint32_t numBytesPerValue;
};

class NodeIDVector {
public:
    // If there is still non-null values after discarding, return true. Otherwise, return false.
    // For an unflat vector, its selection vector is also updated to the resultSelVector.
    static bool discardNull(ValueVector& vector);
};

} // namespace common
} // namespace kuzu
