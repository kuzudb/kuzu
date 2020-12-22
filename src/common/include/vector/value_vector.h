#pragma once

#include "src/common/include/types.h"

using namespace graphflow::common;

namespace graphflow {
namespace common {

//! A Vector represents values of the same data type.
class ValueVector {

public:
    ValueVector(int elementSize) { this->buffer = new uint8_t[VECTOR_CAPACITY * elementSize]; }

    ~ValueVector() { delete buffer; }

    uint8_t* getValues() { return values; }
    void setValues(uint8_t* values) { this->values = values; }

    void reset() { values = buffer; }

protected:
    ValueVector() {}

    uint8_t* values;
    uint8_t* buffer;

public:
    //! The capacity of vector values is dependent on how the vector is produced.
    //!  Scans produce vectors in chunks of 1024 nodes while extends leads to the
    //!  the max size of an adjacency list which is 2048 nodes.
    constexpr static size_t VECTOR_CAPACITY = 2048;
    constexpr static size_t NODE_SEQUENCE_VECTOR_SIZE = 1024;
};

} // namespace common
} // namespace graphflow
