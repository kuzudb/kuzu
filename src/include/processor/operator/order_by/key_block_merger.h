#pragma once

#include <queue>

#include "processor/operator/order_by/order_by_key_encoder.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

struct KeyBlockMergeMorsel;

// This struct stores the string key column information. We can utilize the
// pre-computed indexes and offsets to expedite the tuple comparison in merge sort.
struct StrKeyColInfo {
    StrKeyColInfo(uint32_t colOffsetInFT, uint32_t colOffsetInEncodedKeyBlock, bool isAscOrder)
        : colOffsetInFT{colOffsetInFT}, colOffsetInEncodedKeyBlock{colOffsetInEncodedKeyBlock},
          isAscOrder{isAscOrder} {}

    inline uint32_t getEncodingSize() const {
        return OrderByKeyEncoder::getEncodingSize(DataType(STRING));
    }

    uint32_t colOffsetInFT;
    uint32_t colOffsetInEncodedKeyBlock;
    bool isAscOrder;
};

class MergedKeyBlocks {
public:
    MergedKeyBlocks(uint32_t numBytesPerTuple, uint64_t numTuples, MemoryManager* memoryManager);

    // This constructor is used to convert a dataBlock to a MergedKeyBlocks.
    MergedKeyBlocks(uint32_t numBytesPerTuple, shared_ptr<DataBlock> keyBlock);

    inline uint8_t* getTuple(uint64_t tupleIdx) const {
        assert(tupleIdx < numTuples);
        return keyBlocks[tupleIdx / numTuplesPerBlock]->getData() +
               numBytesPerTuple * (tupleIdx % numTuplesPerBlock);
    }

    inline uint64_t getNumTuples() const { return numTuples; }

    inline uint32_t getNumBytesPerTuple() const { return numBytesPerTuple; }

    inline uint32_t getNumTuplesPerBlock() const { return numTuplesPerBlock; }

    inline uint8_t* getKeyBlockBuffer(uint32_t idx) const {
        assert(idx < keyBlocks.size());
        return keyBlocks[idx]->getData();
    }

    uint8_t* getBlockEndTuplePtr(
        uint32_t blockIdx, uint64_t endTupleIdx, uint32_t endTupleBlockIdx) const;

private:
    uint32_t numBytesPerTuple;
    uint32_t numTuplesPerBlock;
    uint64_t numTuples;
    vector<shared_ptr<DataBlock>> keyBlocks;
    uint32_t endTupleOffset;
};

struct BlockPtrInfo {
    inline BlockPtrInfo(
        uint64_t startTupleIdx, uint64_t endTupleIdx, shared_ptr<MergedKeyBlocks>& keyBlocks)
        : keyBlocks{keyBlocks}, curTuplePtr{keyBlocks->getTuple(startTupleIdx)},
          curBlockIdx{startTupleIdx / keyBlocks->getNumTuplesPerBlock()},
          endBlockIdx{endTupleIdx == 0 ? 0 : (endTupleIdx - 1) / keyBlocks->getNumTuplesPerBlock()},
          curBlockEndTuplePtr{
              keyBlocks->getBlockEndTuplePtr(curBlockIdx, endTupleIdx, endBlockIdx)},
          endTuplePtr{keyBlocks->getBlockEndTuplePtr(endBlockIdx, endTupleIdx, endBlockIdx)},
          endTupleIdx{endTupleIdx} {}

    inline bool hasMoreTuplesToRead() const { return curTuplePtr != endTuplePtr; }

    inline uint64_t getNumBytesLeftInCurBlock() const { return curBlockEndTuplePtr - curTuplePtr; }

    inline uint64_t getNumTuplesLeftInCurBlock() const {
        return getNumBytesLeftInCurBlock() / keyBlocks->getNumBytesPerTuple();
    }

    void updateTuplePtrIfNecessary();

    shared_ptr<MergedKeyBlocks>& keyBlocks;
    uint8_t* curTuplePtr;
    uint64_t curBlockIdx;
    uint64_t endBlockIdx;
    uint8_t* curBlockEndTuplePtr;
    uint8_t* endTuplePtr;
    uint64_t endTupleIdx;
};

class KeyBlockMerger {
public:
    explicit KeyBlockMerger(vector<shared_ptr<FactorizedTable>>& factorizedTables,
        vector<StrKeyColInfo>& strKeyColsInfo, uint32_t numBytesPerTuple)
        : factorizedTables{factorizedTables}, strKeyColsInfo{strKeyColsInfo},
          numBytesPerTuple{numBytesPerTuple}, numBytesToCompare{numBytesPerTuple - 8},
          hasStringCol{!strKeyColsInfo.empty()} {}

    void mergeKeyBlocks(KeyBlockMergeMorsel& keyBlockMergeMorsel) const;

    inline bool compareTuplePtr(uint8_t* leftTuplePtr, uint8_t* rightTuplePtr) const {
        return hasStringCol ? compareTuplePtrWithStringCol(leftTuplePtr, rightTuplePtr) :
                              memcmp(leftTuplePtr, rightTuplePtr, numBytesToCompare) > 0;
    }

    bool compareTuplePtrWithStringCol(uint8_t* leftTuplePtr, uint8_t* rightTuplePtr) const;

private:
    void copyRemainingBlockDataToResult(BlockPtrInfo& blockToCopy, BlockPtrInfo& resultBlock) const;

private:
    // FactorizedTables[i] stores all order_by columns encoded and sorted by the ith thread.
    // MergeSort uses factorizedTable to access the full contents of the string key columns
    // when resolving ties.
    vector<shared_ptr<FactorizedTable>>& factorizedTables;
    // We also store the colIdxInFactorizedTable, colOffsetInEncodedKeyBlock, isAscOrder, isStrCol
    // for each string column. So, we don't need to compute them again during merge sort.
    vector<StrKeyColInfo>& strKeyColsInfo;
    uint32_t numBytesPerTuple;
    uint32_t numBytesToCompare;
    bool hasStringCol;
};

class KeyBlockMergeTask {
public:
    KeyBlockMergeTask(shared_ptr<MergedKeyBlocks> leftKeyBlock,
        shared_ptr<MergedKeyBlocks> rightKeyBlock, shared_ptr<MergedKeyBlocks> resultKeyBlock,
        KeyBlockMerger& keyBlockMerger)
        : leftKeyBlock{move(leftKeyBlock)}, rightKeyBlock{move(rightKeyBlock)},
          resultKeyBlock{move(resultKeyBlock)}, leftKeyBlockNextIdx{0}, rightKeyBlockNextIdx{0},
          activeMorsels{0}, keyBlockMerger{keyBlockMerger} {}

    unique_ptr<KeyBlockMergeMorsel> getMorsel();

    inline bool hasMorselLeft() const {
        // Returns true if there are still morsels left in the current task.
        return leftKeyBlockNextIdx < leftKeyBlock->getNumTuples() ||
               rightKeyBlockNextIdx < rightKeyBlock->getNumTuples();
    }

private:
    uint64_t findRightKeyBlockIdx(uint8_t* leftEndTuplePtr);

public:
    static const uint32_t batch_size = 10000;

    shared_ptr<MergedKeyBlocks> leftKeyBlock;
    shared_ptr<MergedKeyBlocks> rightKeyBlock;
    shared_ptr<MergedKeyBlocks> resultKeyBlock;
    uint64_t leftKeyBlockNextIdx;
    uint64_t rightKeyBlockNextIdx;
    // The counter is used to keep track of the number of morsels given to thread.
    // If the counter is 0 and there is no morsel left in the current task, we can
    // put the resultKeyBlock back to the keyBlock list.
    uint64_t activeMorsels;
    // KeyBlockMerger is used to compare the values of two tuples during the binary search.
    KeyBlockMerger& keyBlockMerger;
};

struct KeyBlockMergeMorsel {
    explicit KeyBlockMergeMorsel(uint64_t leftKeyBlockStartIdx, uint64_t leftKeyBlockEndIdx,
        uint64_t rightKeyBlockStartIdx, uint64_t rightKeyBlockEndIdx)
        : leftKeyBlockStartIdx{leftKeyBlockStartIdx}, leftKeyBlockEndIdx{leftKeyBlockEndIdx},
          rightKeyBlockStartIdx{rightKeyBlockStartIdx}, rightKeyBlockEndIdx{rightKeyBlockEndIdx} {}

    shared_ptr<KeyBlockMergeTask> keyBlockMergeTask;
    uint64_t leftKeyBlockStartIdx;
    uint64_t leftKeyBlockEndIdx;
    uint64_t rightKeyBlockStartIdx;
    uint64_t rightKeyBlockEndIdx;
};

// A dispatcher class used to assign KeyBlockMergeMorsel to threads.
// All functions are guaranteed to be thread-safe, so callers don't need to
// acquire a lock before calling these functions.
class KeyBlockMergeTaskDispatcher {
public:
    inline bool isDoneMerge() {
        lock_guard<mutex> keyBlockMergeDispatcherLock{mtx};
        // Returns true if there are no more merge task to do or the sortedKeyBlocks is empty
        // (meaning that the resultSet is empty).
        return sortedKeyBlocks->size() <= 1 && activeKeyBlockMergeTasks.empty();
    }

    unique_ptr<KeyBlockMergeMorsel> getMorsel();

    void doneMorsel(unique_ptr<KeyBlockMergeMorsel> morsel);

    // This function is used to initialize the columns of keyBlockMergeTaskDispatcher based on
    // sharedFactorizedTablesAndSortedKeyBlocks.
    void init(MemoryManager* memoryManager,
        shared_ptr<queue<shared_ptr<MergedKeyBlocks>>> sortedKeyBlocks,
        vector<shared_ptr<FactorizedTable>>& factorizedTables,
        vector<StrKeyColInfo>& strKeyColsInfo, uint64_t numBytesPerTuple);

private:
    mutex mtx;

    MemoryManager* memoryManager;
    shared_ptr<queue<shared_ptr<MergedKeyBlocks>>> sortedKeyBlocks;
    vector<shared_ptr<KeyBlockMergeTask>> activeKeyBlockMergeTasks;
    unique_ptr<KeyBlockMerger> keyBlockMerger;
};

} // namespace processor
} // namespace kuzu
