#pragma once

#include "src/common/include/types.h"

using namespace graphflow::common;

namespace graphflow {
namespace common {

//! A Vector represents values of the same data type.
class ValueVector {

public:
    ValueVector(const uint64_t& elementSize) : ValueVector{elementSize, VECTOR_CAPACITY} {}

    uint8_t* getValues() const { return values; }
    void setValues(uint8_t* ptrInFrame) { this->values = ptrInFrame; }

    void reset() { values = buffer.get(); }

    uint8_t* reserve(size_t size) {
        if (this->size < size) {
            auto newBuffer = new uint8_t[size];
            memcpy(newBuffer, buffer.get(), this->size);
            buffer.reset(newBuffer);
            this->size = size;
        }
        return buffer.get();
    }

protected:
    ValueVector(const uint64_t& elementSize, const uint64_t& capacity)
        : size{capacity * elementSize},
          buffer(make_unique<uint8_t[]>(size)), values{buffer.get()} {};

public:
    //! The capacity of vector values is dependent on how the vector is produced.
    //!  Scans produce vectors in chunks of 1024 nodes while extends leads to the
    //!  the max size of an adjacency list which is 2048 nodes.
    constexpr static size_t VECTOR_CAPACITY = 2048;
    constexpr static size_t NODE_SEQUENCE_VECTOR_SIZE = 1024;

protected:
    size_t size;
    unique_ptr<uint8_t[]> buffer;
    uint8_t* values;
};

} // namespace common
} // namespace graphflow
