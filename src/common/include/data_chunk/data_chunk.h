#pragma once

#include <memory>
#include <vector>

#include "src/common/include/data_chunk/data_chunk_state.h"
#include "src/common/include/types.h"
#include "src/common/include/vector/value_vector.h"

using namespace std;

namespace graphflow {
namespace common {

// A DataChunk represents tuples as a set of value vectors and a selector array.
// The data chunk represents a subset of a relation i.e., a set of tuples as
// lists of the same length. It is appended into DataChunks and passed as intermediate
// representations between operators.
// A data chunk further contains a DataChunkState, which keeps the data chunk's size, selector, and
// currIdx (used when flattening and implies the value vector only contains the elements at currIdx
// of each value vector).
class DataChunk {

public:
    DataChunk()
        : DataChunk(shared_ptr<DataChunkState>(new DataChunkState(DEFAULT_VECTOR_CAPACITY))){};

    DataChunk(shared_ptr<DataChunkState> _state) : state{_state} {};

    void append(shared_ptr<ValueVector> valueVector) {
        valueVector->state = this->state;
        valueVectors.push_back(valueVector);
    }

    inline shared_ptr<ValueVector> getValueVector(uint64_t valueVectorPos) {
        return valueVectors[valueVectorPos];
    }

    unique_ptr<DataChunk> clone();

public:
    vector<shared_ptr<ValueVector>> valueVectors;
    shared_ptr<DataChunkState> state;
};

} // namespace common
} // namespace graphflow
