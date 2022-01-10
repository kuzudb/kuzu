#pragma once
#include <queue>

#include "src/processor/include/physical_plan/operator/order_by/order_by_key_encoder.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

struct KeyBlockMergeMorsel;

// This struct stores the string and unstructured key column information. We can utilize the
// pre-computed indexes and offsets to expedite the row comparison in merge sort.
struct StringAndUnstructuredKeyColInfo {
    explicit StringAndUnstructuredKeyColInfo(uint64_t colOffsetInRowCollection,
        uint64_t colOffsetInEncodedKeyBlock, bool isAscOrder, bool isStrCol)
        : colOffsetInRowCollection{colOffsetInRowCollection},
          colOffsetInEncodedKeyBlock{colOffsetInEncodedKeyBlock}, isStrCol{isStrCol},
          isAscOrder{isAscOrder} {}
    uint64_t getEncodingSize() {
        return isStrCol ? OrderByKeyEncoder::getEncodingSize(STRING) :
                          OrderByKeyEncoder::getEncodingSize(UNSTRUCTURED);
    }

    uint64_t colOffsetInRowCollection;
    uint64_t colOffsetInEncodedKeyBlock;
    bool isAscOrder;
    bool isStrCol;
};

class KeyBlockMerger {
public:
    explicit KeyBlockMerger(vector<shared_ptr<RowCollection>>& rowCollections,
        vector<StringAndUnstructuredKeyColInfo>& stringAndUnstructuredKeyColInfo,
        uint64_t keyBlockEntrySizeInBytes)
        : rowCollections{rowCollections},
          stringAndUnstructuredKeyColInfo{stringAndUnstructuredKeyColInfo},
          keyBlockEntrySizeInBytes{keyBlockEntrySizeInBytes} {}

    void mergeKeyBlocks(KeyBlockMergeMorsel& keyBlockMergeMorsel);

    inline uint64_t getKeyBlockEntrySizeInBytes() { return keyBlockEntrySizeInBytes; }

    bool compareRowBuffer(uint8_t* leftRowBuffer, uint8_t* rightRowBuffer);

private:
    inline uint8_t* getValPtrFromRowCollection(
        uint8_t* encodedRowBuffer, uint64_t colOffsetInRowCollection) {
        auto encodedRowInfoBuffer = encodedRowBuffer + keyBlockEntrySizeInBytes - sizeof(uint64_t);
        auto encodedRowIdx = OrderByKeyEncoder::getEncodedRowIdx(encodedRowInfoBuffer);
        auto encodedRowCollectionIdx =
            OrderByKeyEncoder::getEncodedRowCollectionIdx(encodedRowInfoBuffer);
        return rowCollections[encodedRowCollectionIdx]->getRow(encodedRowIdx) +
               colOffsetInRowCollection;
    }

private:
    // RowCollections[i] stores all orderBy columns encoded and sorted by the ith thread.
    // MergeSort uses rowCollection to access the full contents of the string key columns
    // when resolving ties.
    vector<shared_ptr<RowCollection>>& rowCollections;
    // We also store the pre-computed string column information including colOffsetInRowCollection,
    // colIdxInEncodedKeyBlock, colOffsetInEncodedKeyBlock. So, we don't need to compute it again
    // during merge sort.
    vector<StringAndUnstructuredKeyColInfo>& stringAndUnstructuredKeyColInfo;
    uint64_t keyBlockEntrySizeInBytes;
};

class KeyBlockMergeTask {
public:
    explicit KeyBlockMergeTask(shared_ptr<KeyBlock> leftKeyBlock,
        shared_ptr<KeyBlock> rightKeyBlock, shared_ptr<KeyBlock> resultKeyBlock,
        KeyBlockMerger& keyBlockMerger)
        : leftKeyBlock{leftKeyBlock}, rightKeyBlock{rightKeyBlock}, resultKeyBlock{resultKeyBlock},
          leftKeyBlockNextIdx{0}, rightKeyBlockNextIdx{0}, activeMorsels{0}, keyBlockMerger{
                                                                                 keyBlockMerger} {}

    unique_ptr<KeyBlockMergeMorsel> getMorsel();

    inline bool hasMorselLeft() {
        // Returns true if there are still morsels left in the current task.
        return leftKeyBlockNextIdx < leftKeyBlock->numEntriesInMemBlock ||
               rightKeyBlockNextIdx < rightKeyBlock->numEntriesInMemBlock;
    }

private:
    uint64_t findRightKeyBlockIdx(uint8_t* leftEndRowBuffer);

public:
    static const uint64_t batch_size = 100;

    shared_ptr<KeyBlock> leftKeyBlock;
    shared_ptr<KeyBlock> rightKeyBlock;
    shared_ptr<KeyBlock> resultKeyBlock;
    uint64_t leftKeyBlockNextIdx;
    uint64_t rightKeyBlockNextIdx;
    // The counter is used to keep track of the number of morsels given to thread.
    // If the counter is 0 and there is no morsel left in the current task, we can
    // put the resultKeyBlock back to the keyBlock list.
    uint64_t activeMorsels;
    // KeyBlockMerger is used to compare the values of two rows during the binary search.
    KeyBlockMerger& keyBlockMerger;
};

struct KeyBlockMergeMorsel {
    explicit KeyBlockMergeMorsel(uint64_t leftKeyBlockStartIdx, uint64_t leftKeyBlockEndIdx,
        uint64_t rightKeyBlockStartIdx, uint64_t rightKeyBlockEndIdx)
        : leftKeyBlockStartIdx{leftKeyBlockStartIdx}, leftKeyBlockEndIdx{leftKeyBlockEndIdx},
          rightKeyBlockStartIdx{rightKeyBlockStartIdx}, rightKeyBlockEndIdx{rightKeyBlockEndIdx} {}

    inline uint8_t* getLeftKeyBlockData() {
        return keyBlockMergeTask->leftKeyBlock->getMemBlockData();
    }
    inline uint8_t* getRightKeyBlockData() {
        return keyBlockMergeTask->rightKeyBlock->getMemBlockData();
    }
    inline uint8_t* getResultKeyBlockData() {
        return keyBlockMergeTask->resultKeyBlock->getMemBlockData();
    }

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

    // This function is used to initialize the fields of keyBlockMergeTaskDispatcher based on
    // sharedRowCollectionsAndSortedKeyBlocks. If the class is already initialized, then it
    // just returns.
    void initIfNecessary(MemoryManager* memoryManager,
        shared_ptr<queue<shared_ptr<KeyBlock>>> sortedKeyBlocks,
        vector<shared_ptr<RowCollection>>& rowCollections,
        vector<StringAndUnstructuredKeyColInfo>& stringAndUnstructuredKeyColInfo,
        uint64_t keyBlockEntrySizeInBytes) {
        lock_guard<mutex> keyBlockMergeDispatcherLock{mtx};
        if (isInitialized) {
            return;
        }
        isInitialized = true;
        this->memoryManager = memoryManager;
        this->sortedKeyBlocks = sortedKeyBlocks;
        this->keyBlockMerger = make_unique<KeyBlockMerger>(
            rowCollections, stringAndUnstructuredKeyColInfo, keyBlockEntrySizeInBytes);
    }

private:
    mutex mtx;

    bool isInitialized = false;
    MemoryManager* memoryManager;
    shared_ptr<queue<shared_ptr<KeyBlock>>> sortedKeyBlocks;
    vector<shared_ptr<KeyBlockMergeTask>> activeKeyBlockMergeTasks;
    unique_ptr<KeyBlockMerger> keyBlockMerger;
};

} // namespace processor
} // namespace graphflow
