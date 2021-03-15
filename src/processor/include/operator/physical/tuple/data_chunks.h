#pragma once

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/vector/node_vector.h"
#include "src/storage/include/structures/common.h"

using namespace graphflow::common;
using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class DataChunks {

public:
    void append(shared_ptr<DataChunk> dataChunk, shared_ptr<ListSyncState> listSyncer) {
        dataChunks.push_back(dataChunk);
        listSyncStatesPerDataChunk.push_back(listSyncer);
    }

    void append(shared_ptr<DataChunk> dataChunk) { append(dataChunk, nullptr); }

    uint64_t getNumTuples();

    shared_ptr<DataChunk> getDataChunk(const uint64_t& dataChunkPos) {
        return dataChunks[dataChunkPos];
    }

    shared_ptr<ListSyncState> getListSyncState(const uint64_t& dataChunkPos) {
        return listSyncStatesPerDataChunk[dataChunkPos];
    }

    uint64_t getNumDataChunks() { return dataChunks.size(); }

    uint64_t getNumValueVectors(const uint64_t& inDataChunkPos) {
        return dataChunks[inDataChunkPos]->getNumAttributes();
    }

private:
    vector<shared_ptr<DataChunk>> dataChunks;
    vector<shared_ptr<ListSyncState>> listSyncStatesPerDataChunk;
};

} // namespace processor
} // namespace graphflow
