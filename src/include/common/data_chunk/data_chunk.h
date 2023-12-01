#pragma once

#include <memory>
#include <vector>

#include "common/data_chunk/data_chunk_state.h"
#include "common/vector/value_vector.h"

namespace kuzu {
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
    explicit DataChunk(uint32_t numValueVectors)
        : DataChunk(numValueVectors, std::make_shared<DataChunkState>()){};

    DataChunk(uint32_t numValueVectors, const std::shared_ptr<DataChunkState>& state)
        : valueVectors(numValueVectors), state{state} {};

    void insert(uint32_t pos, std::shared_ptr<ValueVector> valueVector);

    void resetAuxiliaryBuffer();

    inline uint32_t getNumValueVectors() const { return valueVectors.size(); }

    inline std::shared_ptr<ValueVector> getValueVector(uint64_t valueVectorPos) {
        return valueVectors[valueVectorPos];
    }

public:
    std::vector<std::shared_ptr<ValueVector>> valueVectors;
    std::shared_ptr<DataChunkState> state;
};

} // namespace common
} // namespace kuzu
