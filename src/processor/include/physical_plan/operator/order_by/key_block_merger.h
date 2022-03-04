#pragma once
#include <queue>

#include "src/processor/include/physical_plan/operator/order_by/order_by_key_encoder.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

struct KeyBlockMergeMorsel;

// This struct stores the string and unstructured key column information. We can utilize the
// pre-computed indexes and offsets to expedite the tuple comparison in merge sort.
struct StringAndUnstructuredKeyColInfo {
    explicit StringAndUnstructuredKeyColInfo(uint64_t colIdxInFactorizedTable,
        uint64_t colOffsetInEncodedKeyBlock, bool isAscOrder, bool isStrCol)
        : colIdxInFactorizedTable{colIdxInFactorizedTable},
          colOffsetInEncodedKeyBlock{colOffsetInEncodedKeyBlock}, isStrCol{isStrCol},
          isAscOrder{isAscOrder} {}
    uint64_t getEncodingSize() {
        return isStrCol ? OrderByKeyEncoder::getEncodingSize(STRING) :
                          OrderByKeyEncoder::getEncodingSize(UNSTRUCTURED);
    }
    uint64_t colIdxInFactorizedTable;
    uint64_t colOffsetInEncodedKeyBlock;
    bool isAscOrder;
    bool isStrCol;
};

struct MergedKeyBlocks {
public:
    MergedKeyBlocks(uint64_t numBytesPerTuple, uint64_t numTuples, MemoryManager* memoryManager);

    // This constructor is used to convert a dataBlock to a MergedKeyBlocks.
    MergedKeyBlocks(uint64_t numBytesPerTuple, shared_ptr<DataBlock> keyBlock);

    inline uint8_t* getTuple(uint64_t tupleIdx) const {
        return keyBlocks[tupleIdx / (LARGE_PAGE_SIZE / numBytesPerTuple)]->getData() +
               numBytesPerTuple * (tupleIdx % (LARGE_PAGE_SIZE / numBytesPerTuple));
    }

    inline uint64_t getNumTuples() { return numTuples; }

    inline uint64_t getNumBytesPerTuple() { return numBytesPerTuple; }

private:
    uint64_t numBytesPerTuple;
    uint64_t numTuples;
    vector<shared_ptr<DataBlock>> keyBlocks;
};

class KeyBlockMerger {
public:
    explicit KeyBlockMerger(vector<shared_ptr<FactorizedTable>>& factorizedTables,
        vector<StringAndUnstructuredKeyColInfo>& stringAndUnstructuredKeyColInfo,
        uint64_t numBytesPerTuple)
        : factorizedTables{factorizedTables},
          stringAndUnstructuredKeyColInfo{stringAndUnstructuredKeyColInfo}, numBytesPerTuple{
                                                                                numBytesPerTuple} {}

    void mergeKeyBlocks(KeyBlockMergeMorsel& keyBlockMergeMorsel) const;

    bool compareTupleBuffer(uint8_t* leftTupleBuffer, uint8_t* rightTupleBuffer) const;

private:
    pair<uint64_t, uint64_t> getEncodedFactorizedTableIdxAndTupleIdx(
        uint8_t* encodedTupleBuffer) const;

private:
    // FactorizedTables[i] stores all orderBy columns encoded and sorted by the ith thread.
    // MergeSort uses factorizedTable to access the full contents of the string key columns
    // when resolving ties.
    vector<shared_ptr<FactorizedTable>>& factorizedTables;
    // We also store the colIdxInFactorizedTable, colOffsetInEncodedKeyBlock, isAscOrder, isStrCol
    // for each string and unstructured column. So, we don't need to compute them again during merge
    // sort.
    vector<StringAndUnstructuredKeyColInfo>& stringAndUnstructuredKeyColInfo;
    uint64_t numBytesPerTuple;
};

class KeyBlockMergeTask {
public:
    explicit KeyBlockMergeTask(shared_ptr<MergedKeyBlocks> leftKeyBlock,
        shared_ptr<MergedKeyBlocks> rightKeyBlock, shared_ptr<MergedKeyBlocks> resultKeyBlock,
        KeyBlockMerger& keyBlockMerger)
        : leftKeyBlock{leftKeyBlock}, rightKeyBlock{rightKeyBlock}, resultKeyBlock{resultKeyBlock},
          leftKeyBlockNextIdx{0}, rightKeyBlockNextIdx{0}, activeMorsels{0}, keyBlockMerger{
                                                                                 keyBlockMerger} {}

    unique_ptr<KeyBlockMergeMorsel> getMorsel();

    inline bool hasMorselLeft() {
        // Returns true if there are still morsels left in the current task.
        return leftKeyBlockNextIdx < leftKeyBlock->getNumTuples() ||
               rightKeyBlockNextIdx < rightKeyBlock->getNumTuples();
    }

private:
    uint64_t findRightKeyBlockIdx(uint8_t* leftEndTupleBuffer);

public:
    static const uint64_t batch_size = 100;

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
        // Returns true if there are no more merge task to do.
        return sortedKeyBlocks->size() == 1 && activeKeyBlockMergeTasks.empty();
    }

    unique_ptr<KeyBlockMergeMorsel> getMorsel();

    void doneMorsel(unique_ptr<KeyBlockMergeMorsel> morsel);

    // This function is used to initialize the columns of keyBlockMergeTaskDispatcher based on
    // sharedFactorizedTablesAndSortedKeyBlocks. If the class is already initialized, then it
    // just returns.
    void initIfNecessary(MemoryManager* memoryManager,
        shared_ptr<queue<shared_ptr<MergedKeyBlocks>>> sortedKeyBlocks,
        vector<shared_ptr<FactorizedTable>>& factorizedTables,
        vector<StringAndUnstructuredKeyColInfo>& stringAndUnstructuredKeyColInfo,
        uint64_t numBytesPerTuple);

private:
    mutex mtx;

    bool isInitialized = false;
    MemoryManager* memoryManager;
    shared_ptr<queue<shared_ptr<MergedKeyBlocks>>> sortedKeyBlocks;
    vector<shared_ptr<KeyBlockMergeTask>> activeKeyBlockMergeTasks;
    unique_ptr<KeyBlockMerger> keyBlockMerger;
};

} // namespace processor
} // namespace graphflow
