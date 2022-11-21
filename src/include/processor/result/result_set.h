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
    explicit ResultSet(uint32_t numDataChunks)
        : multiplicity{1}, dataChunks(numDataChunks), listSyncStatesPerDataChunk(numDataChunks) {}

    inline void insert(uint32_t pos, const shared_ptr<DataChunk>& dataChunk) {
        assert(dataChunks.size() > pos);
        dataChunks[pos] = dataChunk;
    }

    inline void insert(uint32_t pos, const shared_ptr<ListSyncState>& listSyncState) {
        assert(listSyncStatesPerDataChunk.size() > pos);
        listSyncStatesPerDataChunk[pos] = listSyncState;
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

    shared_ptr<ListSyncState> getListSyncState(uint64_t dataChunkPos) {
        return listSyncStatesPerDataChunk[dataChunkPos];
    }

public:
    uint64_t multiplicity;
    vector<shared_ptr<DataChunk>> dataChunks;

private:
    vector<shared_ptr<ListSyncState>> listSyncStatesPerDataChunk;
};

} // namespace processor
} // namespace kuzu
