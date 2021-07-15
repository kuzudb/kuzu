#pragma once

#include <cassert>

#include "src/common/include/data_chunk/data_chunk_state.h"
#include "src/common/include/types.h"
#include "src/common/include/vector/string_buffer.h"

namespace graphflow {
namespace common {

class NullMask {
    friend class ValueVector;

public:
    explicit NullMask(uint64_t vectorCapacity)
        : mask(make_unique<bool[]>(DEFAULT_VECTOR_CAPACITY)), mayContainNulls{false} {
        fill_n(mask.get(), vectorCapacity, false /* not null */);
    };

    shared_ptr<NullMask> clone(uint64_t vectorCapacity);

private:
    unique_ptr<bool[]> mask;
    bool mayContainNulls;
};

//! A Vector represents values of the same data type.
//! The capacity of a ValueVector is either 1 (single value) or DEFAULT_VECTOR_CAPACITY.
class ValueVector {

public:
    ValueVector(MemoryManager* memoryManager, DataType dataType, bool isSingleValue = false)
        : ValueVector(memoryManager, isSingleValue ? 1 : DEFAULT_VECTOR_CAPACITY,
              TypeUtils::getDataTypeSize(dataType), dataType) {}

    virtual ~ValueVector() = default;

    virtual node_offset_t readNodeOffset(uint64_t pos) const {
        throw invalid_argument("readNodeOffset unsupported.");
    }
    virtual void readNodeID(uint64_t pos, nodeID_t& nodeID) const {
        throw invalid_argument("readNodeID unsupported.");
    }

    void addString(uint64_t pos, string value) const;
    void addString(uint64_t pos, char* value, uint64_t len) const;
    void allocateStringOverflowSpace(gf_string_t& gfString, uint64_t len) const;
    inline void reset() { values = bufferValues.get(); }

    void fillNullMask();

    // Sets the null mask of this ValueVector to the null mask of
    inline void setNullMask(shared_ptr<NullMask> otherMask) { nullMask = otherMask; }

    inline void setAllNull() {
        std::fill(nullMask->mask.get(), nullMask->mask.get() + state->size, true);
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

    inline void setNull(uint64_t pos, bool isNull) {
        nullMask->mask[pos] = isNull;
        nullMask->mayContainNulls |= isNull;
    }

    inline uint8_t isNull(uint64_t pos) { return nullMask->mask[pos]; }

    inline shared_ptr<NullMask> getNullMask() { return nullMask; }
    virtual inline int64_t getNumBytesPerValue() { return TypeUtils::getDataTypeSize(dataType); }

    virtual shared_ptr<ValueVector> clone();

protected:
    ValueVector(MemoryManager* memoryManager, uint64_t vectorCapacity, uint64_t numBytesPerValue,
        DataType dataType)
        : vectorCapacity{vectorCapacity},
          bufferValues(make_unique<uint8_t[]>(numBytesPerValue * vectorCapacity)),
          memoryManager{memoryManager}, dataType{dataType}, values{bufferValues.get()},
          stringBuffer{nullptr}, nullMask{make_shared<NullMask>(vectorCapacity)} {
        if (dataType == STRING || dataType == UNSTRUCTURED) {
            assert(memoryManager);
            stringBuffer = make_unique<StringBuffer>(*memoryManager);
        }
    }

protected:
    uint64_t vectorCapacity;
    unique_ptr<uint8_t[]> bufferValues;
    MemoryManager* memoryManager;

public:
    DataType dataType;
    uint8_t* values;
    unique_ptr<StringBuffer> stringBuffer;
    shared_ptr<DataChunkState> state;

private:
    // This is a shared pointer because sometimes ValueVectors may share NullMasks, e.g., the result
    // ValueVectors of unary expressions, point to the nullMasks of operands.
    shared_ptr<NullMask> nullMask;
};

} // namespace common
} // namespace graphflow
