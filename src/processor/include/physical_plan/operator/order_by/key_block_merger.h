#pragma once

#include "src/processor/include/physical_plan/operator/order_by/order_by_key_encoder.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

// This struct stores the memBlock and the range [startRowIdx,endRowIdx) in the memBlock to merge.
struct KeyBlockMergeInfo {
    explicit KeyBlockMergeInfo(MemoryBlock& memBlock, uint64_t startRowIdx, uint64_t endRowIdx)
        : memBlock{memBlock}, startRowIdx{startRowIdx}, endRowIdx{endRowIdx} {}
    MemoryBlock& memBlock;
    uint64_t startRowIdx;
    uint64_t endRowIdx;
};

// This struct stores the string key column information. We can utilize the pre-computed index and
// offsets to expedite the row comparison in merge sort.
struct StrKeyColInfo {
    explicit StrKeyColInfo(
        uint64_t colOffsetInRowCollection, uint64_t colOffsetInEncodedKeyBlock, bool isAscOrder)
        : colOffsetInRowCollection{colOffsetInRowCollection},
          colOffsetInEncodedKeyBlock{colOffsetInEncodedKeyBlock}, isAscOrder{isAscOrder} {}
    uint64_t colOffsetInRowCollection;
    uint64_t colOffsetInEncodedKeyBlock;
    bool isAscOrder;
};

class KeyBlockMerger {
public:
    explicit KeyBlockMerger(MemoryManager& memoryManager, OrderByKeyEncoder& orderByKeyEncoder,
        vector<unique_ptr<RowCollection>>& rowCollections, vector<StrKeyColInfo>& strKeyColInfo)
        : memoryManager{memoryManager}, orderByKeyEncoder{orderByKeyEncoder},
          rowCollections{rowCollections}, strKeyColInfo{strKeyColInfo} {}

    // This function takes in two memblocks, the ranges of the memblocks to merge, and writes the
    // result to the resultKeyBlock.
    void mergeKeyBlocks(KeyBlockMergeInfo& leftKeyBlockInfo, KeyBlockMergeInfo& rightKeyBlockInfo,
        KeyBlockMergeInfo& resultKeyBlockInfo);

private:
    bool compareRowBuffer(uint8_t* leftRowBuffer, uint8_t* rightRowBuffer);

    inline gf_string_t getStrFromRowCollection(
        uint8_t* encodedRowBuffer, StrKeyColInfo& strKeyColInfo) {
        auto encodedRowInfoBuffer =
            encodedRowBuffer + orderByKeyEncoder.getKeyBlockEntrySizeInBytes() - sizeof(uint64_t);
        auto encodedRowID = orderByKeyEncoder.getEncodedRowID(encodedRowInfoBuffer);
        auto encodedThreadID = orderByKeyEncoder.getEncodedThreadID(encodedRowInfoBuffer);
        return *((gf_string_t*)(rowCollections[encodedThreadID]->getRow(encodedRowID) +
                                strKeyColInfo.colOffsetInRowCollection));
    }

private:
    MemoryManager& memoryManager;
    OrderByKeyEncoder& orderByKeyEncoder;
    // RowCollections[i] stores all orderBy columns encoded and sorted by the ith thread.
    // MergeSort uses rowCollection to access the full contents of the string key columns
    // when resolving ties.
    vector<unique_ptr<RowCollection>>& rowCollections;
    // We also store the pre-computed string column information including colOffsetInRowCollection,
    // colIdxInEncodedKeyBlock, colOffsetInEncodedKeyBlock. So, we don't need to compute it again
    // during merge sort.
    vector<StrKeyColInfo>& strKeyColInfo;
};

} // namespace processor
} // namespace graphflow
