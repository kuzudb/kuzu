#pragma once

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/vector/node_vector.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

class DataChunks {

public:
    void append(shared_ptr<DataChunk> dataChunk) { dataChunks.push_back(dataChunk); }

    uint64_t getNumTuples();

    shared_ptr<DataChunk> getDataChunk(const uint64_t& dataChunkPos) {
        return dataChunks[dataChunkPos];
    }

    uint64_t getNumDataChunks() { return dataChunks.size(); }

    uint64_t getNumValueVectors(const uint64_t& inDataChunkIdx) {
        return dataChunks[inDataChunkIdx]->getNumAttributes();
    }

private:
    vector<shared_ptr<DataChunk>> dataChunks;
};

} // namespace processor
} // namespace graphflow
