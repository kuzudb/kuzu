#pragma once

#include <unordered_set>

#include "common/data_chunk/data_chunk.h"
#include "processor/data_pos.h"
#include "storage/storage_structure/lists/list_sync_state.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

class ResultSet {

public:
    explicit ResultSet(uint32_t numDataChunks) : multiplicity{1}, dataChunks(numDataChunks) {}

    inline void insert(uint32_t pos, shared_ptr<DataChunk> dataChunk) {
        assert(dataChunks.size() > pos);
        dataChunks[pos] = std::move(dataChunk);
    }

    inline shared_ptr<ValueVector> getValueVector(DataPos& dataPos) {
        return dataChunks[dataPos.dataChunkPos]->valueVectors[dataPos.valueVectorPos];
    }

    // Our projection does NOT explicitly remove dataChunk from resultSet. Therefore, caller should
    // always provide a set of positions when reading from multiple dataChunks.
    inline uint64_t getNumTuples(const unordered_set<uint32_t>& dataChunksPosInScope) {
        return getNumTuplesWithoutMultiplicity(dataChunksPosInScope) * multiplicity;
    }

    uint64_t getNumTuplesWithoutMultiplicity(const unordered_set<uint32_t>& dataChunksPosInScope);

public:
    uint64_t multiplicity;
    vector<shared_ptr<DataChunk>> dataChunks;
};

} // namespace processor
} // namespace kuzu
