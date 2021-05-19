#pragma once

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/storage/include/data_structure/column.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::storage;

namespace graphflow {
namespace transaction {

class LocalStorage {

    typedef unordered_map<uint32_t, unique_ptr<uint8_t[]>> page_idx_to_dirty_page_map;

public:
    void addDataChunk(DataChunk* dataChunk) {
        dataChunks.emplace_back(move(dataChunk->clone()));
        numTuples += dataChunk->state->size;
    }

    void assignNodeIDs(label_t nodeLabel);

    void mapNodeIDs(label_t nodeLabel);

    void deleteNodeIDs(label_t nodeLabel);

    void computeCreateNode(vector<uint32_t> propertyKeyIdxToVectorPosMap, label_t nodeLabel,
        vector<BaseColumn*> nodePropertyColumns, uint64_t numNodes);

    void computeUpdateNode(vector<uint32_t> propertyKeyIdxToVectorPosMap, label_t nodeLabel,
        vector<BaseColumn*> nodePropertyColumns, uint64_t numNodes);

    void computeDeleteNode(vector<uint32_t> propertyKeyIdxToVectorPosMap, label_t nodeLabel,
        vector<BaseColumn*> nodePropertyColumns, uint64_t numNodes);

private:
    void updateNodePropertyColumn(uint32_t currPosInDataChunk, uint64_t numUpdatesInDataChunk,
        BaseColumn& column, uint8_t* nullValue, page_idx_to_dirty_page_map& dirtyPagesMap,
        ValueVector* propertyValueVector, NodeIDVector* nodeIDVector);

    void appendToNodePropertyColumn(uint32_t& dataChunkIdx, uint32_t& currPosInDataChunk,
        BaseColumn& column, uint8_t* nullValue, page_idx_to_dirty_page_map& dirtyPagesMap,
        uint32_t dataChunkSize, ValueVector* propertyValueVector, NodeIDVector* nodeIDVector);

    // helper functions
    uint8_t* putPageInDirtyPagesMap(page_idx_to_dirty_page_map& dirtyPagesMap, uint32_t pageIdx,
        FileHandle* fileHandle, bool createEmptyPage);

private:
    vector<unique_ptr<DataChunk>> dataChunks;
    uint64_t numTuples;

    // data structures for local snapshoting `CREATE or UPDATE nodes`
    unordered_map<FileHandle*, page_idx_to_dirty_page_map> uncommittedPages;

    // data structures for local snapshoting `CREATE nodes`
    uint32_t uncommittedNumRecycledNodeIDs = -1;
    unordered_map<FileHandle*, uint64_t> uncommittedFileHandleNumPages;
    unordered_map<label_t, uint64_t> uncommittedLabelToNumNodes;
};

} // namespace transaction
} // namespace graphflow
