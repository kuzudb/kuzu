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
// currPos (used when flattening and implies the value vector only contains the elements at currPos
// of each value vector).
class DataChunk {

public:
    DataChunk() : DataChunk(true){};

    DataChunk(bool initializeSelectedValuesPos)
        : DataChunk(initializeSelectedValuesPos, MAX_VECTOR_SIZE) {}

    DataChunk(shared_ptr<DataChunkState> dataChunkState) : state{dataChunkState} {}

    DataChunk(bool initializeSelectedValuesPos, uint64_t capacity)
        : state{make_shared<DataChunkState>(initializeSelectedValuesPos, capacity)} {};

    void append(shared_ptr<ValueVector> valueVector) {
        valueVector->ownerState = this->state;
        valueVectors.push_back(valueVector);
    }

    inline shared_ptr<ValueVector> getValueVector(uint64_t valueVectorPos) {
        return valueVectors[valueVectorPos];
    }

    inline uint64_t getCurrSelectedValuesPos() { return state->selectedValuesPos[state->currPos]; }

    inline bool isFlat() const { return state->currPos != -1; }

public:
    vector<shared_ptr<ValueVector>> valueVectors;
    shared_ptr<DataChunkState> state;
};

} // namespace common
} // namespace graphflow
