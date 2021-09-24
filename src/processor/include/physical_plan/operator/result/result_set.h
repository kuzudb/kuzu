#pragma once

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/processor/include/physical_plan/data_pos.h"
#include "src/storage/include/data_structure/lists/list_sync_state.h"

using namespace graphflow::common;
using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class ResultSet {

public:
    explicit ResultSet(uint32_t numDataChunks)
        : multiplicity{1}, dataChunks(numDataChunks), dataChunksMask(numDataChunks, true),
          listSyncStatesPerDataChunk(numDataChunks) {}

    inline void insert(uint32_t pos, const shared_ptr<DataChunk>& dataChunk) {
        assert(dataChunks.size() > pos);
        dataChunks[pos] = dataChunk;
    }

    inline void insert(uint32_t pos, const shared_ptr<ListSyncState>& listSyncState) {
        assert(listSyncStatesPerDataChunk.size() > pos);
        listSyncStatesPerDataChunk[pos] = listSyncState;
    }

    inline uint32_t getNumDataChunks() const { return dataChunks.size(); }

    inline shared_ptr<ValueVector> getValueVector(DataPos& dataPos) {
        return dataChunks[dataPos.dataChunkPos]->valueVectors[dataPos.valueVectorPos];
    }

    uint64_t getNumTuples();

    shared_ptr<ListSyncState> getListSyncState(uint64_t dataChunkPos) {
        return listSyncStatesPerDataChunk[dataChunkPos];
    }

public:
    uint64_t multiplicity;
    vector<shared_ptr<DataChunk>> dataChunks;
    // A dataChunk might be projected away and its mask value will be set to false.
    vector<bool> dataChunksMask;

private:
    vector<shared_ptr<ListSyncState>> listSyncStatesPerDataChunk;
};

} // namespace processor
} // namespace graphflow
