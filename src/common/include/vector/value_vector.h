#pragma once

#include <cassert>

#include "src/common/include/types.h"
#include "src/common/include/vector/string_buffer.h"
#include "src/common/include/vector/vector_state.h"

namespace graphflow {
namespace common {

//! A Vector represents values of the same data type.
//! The capacity of a ValueVector is either 1 (single value) or DEFAULT_VECTOR_CAPACITY.
class ValueVector {

public:
    ValueVector(MemoryManager* memoryManager, DataType dataType, bool isSingleValue = false)
        : ValueVector(memoryManager, isSingleValue ? 1 : DEFAULT_VECTOR_CAPACITY,
              getDataTypeSize(dataType), dataType) {}

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

    virtual inline int64_t getNumBytesPerValue() { return getDataTypeSize(dataType); }

    virtual shared_ptr<ValueVector> clone();

protected:
    ValueVector(MemoryManager* memoryManager, uint64_t vectorCapacity, uint64_t numBytesPerValue,
        DataType dataType)
        : vectorCapacity{vectorCapacity},
          bufferValues(make_unique<uint8_t[]>(numBytesPerValue * vectorCapacity)),
          bufferNullMask(make_unique<bool[]>(vectorCapacity)), memoryManager{memoryManager},
          dataType{dataType}, values{bufferValues.get()}, nullMask{bufferNullMask.get()},
          stringBuffer{nullptr} {
        fill_n(nullMask, vectorCapacity, false /* not null */);
        if (dataType == STRING || dataType == UNSTRUCTURED) {
            assert(memoryManager);
            stringBuffer = make_unique<StringBuffer>(*memoryManager);
        }
    }

protected:
    uint64_t vectorCapacity;
    unique_ptr<uint8_t[]> bufferValues;
    unique_ptr<bool[]> bufferNullMask;
    MemoryManager* memoryManager;

public:
    DataType dataType;
    uint8_t* values;
    bool* nullMask;
    unique_ptr<StringBuffer> stringBuffer;
    shared_ptr<VectorState> state;
};

} // namespace common
} // namespace graphflow
