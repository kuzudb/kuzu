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
    void addDataChunk(DataChunk* dataChunk) {
        dataChunks.emplace_back(move(dataChunk->clone()));
        numTuples += dataChunk->state->size;
    }

    void computeCreateNode(vector<uint32_t> propertyKeyIdxToVectorPosMap, label_t nodeLabel,
        vector<BaseColumn*> nodePropertyColumns, uint64_t numNodes);

private:
    uint32_t assignNodeIDs(label_t nodeLabel);

    void updateNodePropertyColumn(uint32_t& dataChunkIdx, uint32_t& idxInDataChunk,
        uint32_t vectorPos, uint32_t& numUpdates, BaseColumn& column, uint8_t* nullValue,
        pageIdxToDirtyPageMap& dirtyPagesMap, uint32_t dataChunkSize,
        ValueVector* propertyValueVector, NodeIDVector* nodeIDVector);

    void appendToNodePropertyColumn(uint32_t& dataChunkIdx, uint32_t& idxInDataChunk,
        uint32_t vectorPos, BaseColumn& column, uint8_t* nullValue,
        pageIdxToDirtyPageMap& dirtyPagesMap, uint32_t dataChunkSize,
        ValueVector* propertyValueVector, NodeIDVector* nodeIDVector);

private:
    vector<unique_ptr<DataChunk>> dataChunks;
    uint64_t numTuples;
    unordered_map<FileHandle*, pageIdxToDirtyPageMap> uncommittedPages;
    unordered_map<label_t, uint64_t> uncommittedLabelToNumNodes;
};

} // namespace transaction
} // namespace graphflow
