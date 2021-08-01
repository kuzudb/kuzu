#pragma once

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/storage/include/data_structure/lists/list_sync_state.h"

using namespace graphflow::common;
using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class ResultSet {

public:
    ResultSet() : multiplicity(1), startOffset{0}, endOffset{UINT64_MAX} {}

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

    inline void setNumTupleToSkip(uint64_t number) { startOffset = number; }
    inline void setNumTupleToLimit(uint64_t number) { endOffset = startOffset + number; }
    inline void resetStartOffSet() { startOffset = 0; }
    inline void resetEndOffset() { endOffset = UINT64_MAX; }
    inline uint64_t getStartOffset() const { return startOffset; }
    inline uint64_t getEndOffset() const { return endOffset; }

public:
    uint64_t multiplicity;
    vector<shared_ptr<DataChunk>> dataChunks;

private:
    vector<shared_ptr<ListSyncState>> listSyncStatesPerDataChunk;
    // Due to SKIP and LIMIT, we might not need to iterate all tuples in result set
    uint64_t startOffset;
    uint64_t endOffset;
};

} // namespace processor
} // namespace graphflow
