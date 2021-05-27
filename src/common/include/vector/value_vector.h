#pragma once

#include "src/common/include/types.h"
#include "src/common/include/vector/vector_state.h"

namespace graphflow {
namespace common {

//! A Vector represents values of the same data type.
class ValueVector {

public:
    ValueVector(DataType dataType, uint64_t vectorCapacity)
        : ValueVector(vectorCapacity, getDataTypeSize(dataType), dataType) {}

    ValueVector(uint64_t numBytesPerValue, DataType dataType)
        : ValueVector(MAX_VECTOR_SIZE, numBytesPerValue, dataType) {}

    ValueVector(DataType dataType) : ValueVector(dataType, MAX_VECTOR_SIZE) {}

    virtual void readNodeOffset(uint64_t pos, nodeID_t& nodeID) const {
        throw invalid_argument("readNodeOffset unsupported.");
    }
    virtual void readNodeOffsetAndLabel(uint64_t pos, nodeID_t& nodeID) const {
        throw invalid_argument("readNodeOffsetAndLabel unsupported.");
    }

    inline void reset() { values = bufferValues.get(); }

    void fillNullMask();

    virtual inline int64_t getNumBytesPerValue() { return getDataTypeSize(dataType); }

    virtual shared_ptr<ValueVector> clone();

protected:
    ValueVector(uint64_t vectorCapacity, uint64_t numBytesPerValue, DataType dataType)
        : capacity{numBytesPerValue * vectorCapacity},
          bufferValues(make_unique<uint8_t[]>(capacity)),
          bufferNullMask(make_unique<bool[]>(vectorCapacity)), dataType{dataType},
          values{bufferValues.get()}, nullMask{bufferNullMask.get()} {
        fill_n(nullMask, vectorCapacity, false /* not null */);
    }

protected:
    size_t capacity;
    unique_ptr<uint8_t[]> bufferValues;
    unique_ptr<bool[]> bufferNullMask;

public:
    DataType dataType;
    uint8_t* values;
    bool* nullMask;
    shared_ptr<VectorState> state;
};

} // namespace common
} // namespace graphflow
