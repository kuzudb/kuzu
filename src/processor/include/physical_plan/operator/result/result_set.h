#pragma once

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/storage/include/data_structure/lists/list_sync_state.h"

using namespace graphflow::common;
using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class ResultSet {

public:
    ResultSet() : multiplicity(1), numTuplesToIterate{UINT64_MAX} {}

    void append(shared_ptr<DataChunk> dataChunk, shared_ptr<ListSyncState> listSyncer) {
        dataChunks.push_back(dataChunk);
        listSyncStatesPerDataChunk.push_back(listSyncer);
    }

    void append(shared_ptr<DataChunk> dataChunk) { append(dataChunk, nullptr); }

    uint64_t getNumTuples();

    unique_ptr<ResultSet> clone();

    shared_ptr<ListSyncState> getListSyncState(uint64_t dataChunkPos) {
        return listSyncStatesPerDataChunk[dataChunkPos];
    }

    inline void setNumTuplesToIterate(uint64_t number) { numTuplesToIterate = number; }

public:
    uint64_t multiplicity;
    vector<shared_ptr<DataChunk>> dataChunks;

private:
    vector<shared_ptr<ListSyncState>> listSyncStatesPerDataChunk;
    // Due to LIMIT, we may not iterate all tuples in a result set. If this field is UINT64_MAX, we
    // iterate all tuples.
    uint64_t numTuplesToIterate;
};

} // namespace processor
} // namespace graphflow
