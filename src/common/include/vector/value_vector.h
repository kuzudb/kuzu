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
    explicit NullMask()
        : mask(make_unique<bool[]>(DEFAULT_VECTOR_CAPACITY)), mayContainNulls{false} {
        fill_n(mask.get(), DEFAULT_VECTOR_CAPACITY, false /* not null */);
    };

    shared_ptr<NullMask> clone();

private:
    unique_ptr<bool[]> mask;
    bool mayContainNulls;
};

//! A Vector represents values of the same data type.
//! The capacity of a ValueVector is either 1 (single value) or DEFAULT_VECTOR_CAPACITY.
class ValueVector {

public:
    ValueVector(MemoryManager* memoryManager, DataType dataType, bool isSequence = false)
        : ValueVector(memoryManager, TypeUtils::getDataTypeSize(dataType), dataType, isSequence) {}

    ~ValueVector() = default;

    void addString(uint64_t pos, string value) const;
    void addString(uint64_t pos, char* value, uint64_t len) const;
    void allocateStringOverflowSpace(gf_string_t& gfString, uint64_t len) const;

    void fillNullMask();

    // Sets the null mask of this ValueVector to the null mask of
    inline void setNullMask(const shared_ptr<NullMask>& otherMask) { nullMask = otherMask; }

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

    inline void setNull(uint64_t pos, bool isNull) {
        nullMask->mask[pos] = isNull;
        nullMask->mayContainNulls |= isNull;
    }

    inline uint8_t isNull(uint64_t pos) { return nullMask->mask[pos]; }

    inline shared_ptr<NullMask> getNullMask() { return nullMask; }
    inline int64_t getNumBytesPerValue() const { return TypeUtils::getDataTypeSize(dataType); }

    // Node specific functions.
    bool discardNullNodes();
    void readNodeID(uint64_t pos, nodeID_t& nodeID) const;
    inline node_offset_t readNodeOffset(uint64_t pos) const {
        assert(dataType == NODE);
        return isSequence ? ((nodeID_t*)values)[0].offset + pos : ((nodeID_t*)values)[pos].offset;
    }

    shared_ptr<ValueVector> clone();

protected:
    ValueVector(
        MemoryManager* memoryManager, uint64_t numBytesPerValue, DataType dataType, bool isSequence)
        : bufferValues(make_unique<uint8_t[]>(numBytesPerValue * DEFAULT_VECTOR_CAPACITY)),
          memoryManager{memoryManager}, dataType{dataType}, values{bufferValues.get()},
          stringBuffer{nullptr}, isSequence{isSequence}, nullMask{make_shared<NullMask>()} {
        if (dataType == STRING || dataType == UNSTRUCTURED) {
            assert(memoryManager);
            stringBuffer = make_unique<StringBuffer>(*memoryManager);
        }
    }

protected:
    unique_ptr<uint8_t[]> bufferValues;
    MemoryManager* memoryManager;

public:
    DataType dataType;
    uint8_t* values;
    unique_ptr<StringBuffer> stringBuffer;
    shared_ptr<DataChunkState> state;
    // Node specific field.
    bool isSequence;

private:
    // This is a shared pointer because sometimes ValueVectors may share NullMasks, e.g., the result
    // ValueVectors of unary expressions, point to the nullMasks of operands.
    shared_ptr<NullMask> nullMask;
};

} // namespace common
} // namespace graphflow
