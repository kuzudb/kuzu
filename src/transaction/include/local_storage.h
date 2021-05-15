#pragma once

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/storage/include/data_structure/column.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::storage;

namespace graphflow {
namespace transaction {

class LocalStorage {

    typedef unordered_map<uint32_t, unique_ptr<uint8_t[]>> pageIdxToDirtyPageMap;

public:
    void addDataChunk(DataChunk* dataChunk) { dataChunks.emplace_back(move(dataChunk->clone())); }

    // TODO: Implements
    void computeCreateNode(vector<uint32_t> propertyKeyIdxToVectorPosMap, label_t nodeLabel,
        vector<BaseColumn*> nodePropertyColumns, uint64_t numNodes) {
        // compute mem pages and metadata changes that is required as a result of create node
        // operation.
        dataChunks.clear();
    }

private:
    vector<unique_ptr<DataChunk>> dataChunks;
};

} // namespace transaction
} // namespace graphflow
