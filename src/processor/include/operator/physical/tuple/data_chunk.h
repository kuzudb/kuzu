#pragma once

#include <vector>

#include "src/common/include/vector/value_vector.h"

using namespace std;

namespace graphflow {
namespace processor {

//! A DataChunk represents tuples as a set of value vectors and a selector array. It represents a
//! subset of a relation i.e., a set of tuples as lists of the same length. DataChunks are passed
//! as intermediate representations between operators.A dataChunk further contains a curr_idx
//! which is used when flatting and implies the value vector only contains the elements at curr_idx
//! of each value vector.
class DataChunk {

public:
    DataChunk() : size{0}, curr_idx{0}, isFlat{false} {};

    void append(shared_ptr<ValueVector> valueVector) { valueVectors.push_back(valueVector); }

    void setAsFlat() { isFlat = true; }

    uint64_t getNumTuples() const { return isFlat && size > 1 ? 1 : size; }

    uint64_t getNumAttributes() const { return valueVectors.size(); }

    shared_ptr<ValueVector> getValueVector(const uint64_t& valueVectorPos) {
        return valueVectors[valueVectorPos];
    }

public:
    vector<shared_ptr<ValueVector>> valueVectors;
    uint64_t size;
    //! The current position when vectors are flattened.
    uint64_t curr_idx;
    bool isFlat;
};

} // namespace processor
} // namespace graphflow
