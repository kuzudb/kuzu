#pragma once

#include "src/common/include/types.h"

using namespace graphflow::common;

namespace graphflow {
namespace common {

//! The capacity of vector values is dependent on how the vector is produced.
//!  Scans produce vectors in chunks of 1024 nodes while extends leads to the
//!  the max size of an adjacency list which is 2048 nodes.
#define NODE_SEQUENCE_VECTOR_SIZE 1024
#define VECTOR_CAPACITY 2048

//! A Vector represents values of the same data type.
class ValueVector {
public:
    ValueVector(int elementSize) {
        this->buffer = new uint8_t[VECTOR_CAPACITY * elementSize];
        this->storedSequentially = false;
    }

    uint8_t* getValues() { return values; }
    void setValues(uint8_t* values) { this->values = values; }

    void reset() { values = buffer; }

    bool isStoredSequentially() { return storedSequentially; }
    void setIsStoredSequentially(bool storedSequentially) {
        this->storedSequentially = storedSequentially;
    }

protected:
    ValueVector() {}

    uint8_t* values;
    uint8_t* buffer;
    bool storedSequentially;
};

} // namespace common
} // namespace graphflow
