#pragma once

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/storage/include/data_structure/lists/list_sync_state.h"

using namespace graphflow::common;
using namespace graphflow::storage;

namespace graphflow {
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

    uint64_t getNumTuples();

    unique_ptr<ResultSet> clone();

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
} // namespace graphflow
