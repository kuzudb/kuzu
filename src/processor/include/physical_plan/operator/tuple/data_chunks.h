#pragma once

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/vector/node_vector.h"
#include "src/storage/include/data_structure/lists/list_sync_state.h"

using namespace graphflow::common;
using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class DataChunks {

public:
    DataChunks() : multiplicity(1) {}

    void append(shared_ptr<DataChunk> dataChunk, shared_ptr<ListSyncState> listSyncer) {
        dataChunks.push_back(dataChunk);
        listSyncStatesPerDataChunk.push_back(listSyncer);
    }

    void append(shared_ptr<DataChunk> dataChunk) { append(dataChunk, nullptr); }

    uint64_t getNumTuples();

    inline shared_ptr<DataChunk> getDataChunk(uint64_t dataChunkPos) {
        return dataChunks[dataChunkPos];
    }

    inline shared_ptr<DataChunkState> getDataChunkState(uint64_t dataChunkPos) {
        return dataChunks[dataChunkPos]->state;
    }

    shared_ptr<ListSyncState> getListSyncState(uint64_t dataChunkPos) {
        return listSyncStatesPerDataChunk[dataChunkPos];
    }

    inline uint64_t getNumDataChunks() { return dataChunks.size(); }

public:
    uint64_t multiplicity;

private:
    vector<shared_ptr<DataChunk>> dataChunks;
    vector<shared_ptr<ListSyncState>> listSyncStatesPerDataChunk;
};

} // namespace processor
} // namespace graphflow
