#pragma once
#include <queue>

#include "src/common/include/configs.h"
#include "src/common/include/vector/value_vector.h"
#include "src/processor/include/physical_plan/operator/order_by/key_block_merger.h"
#include "src/processor/include/physical_plan/operator/order_by/order_by_key_encoder.h"
#include "src/processor/include/physical_plan/result/row_collection.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

struct TieRange {
public:
    uint64_t startingRowIdx;
    uint64_t endingRowIdx;
    inline uint64_t getNumRows() { return endingRowIdx - startingRowIdx + 1; }
    explicit TieRange(uint64_t startingRowIdx, uint64_t endingRowIdx)
        : startingRowIdx{startingRowIdx}, endingRowIdx{endingRowIdx} {}
};

// RadixSort sorts a block of binary strings using the radixSort and quickSort (only for comparing
// string overflow pointers). The algorithm loops through each column of the orderByVectors. If it
// sees a column with string type, which is variable length, it will call radixSort to sort the
// columns seen so far. If there are tie rows, it will then compare the overflow pointers of
// strings. For subsequent columns, the algorithm only calls radixSort on tie rows.
class RadixSort {
public:
    explicit RadixSort(MemoryManager& memoryManager, RowCollection& rowCollection,
        OrderByKeyEncoder& orderByKeyEncoder, vector<StrKeyColInfo> strKeyColInfo)
        : rowCollection{rowCollection}, tmpKeyBlock{memoryManager.allocateBlock(SORT_BLOCK_SIZE)},
          orderByKeyEncoder{orderByKeyEncoder}, strKeyColInfo{strKeyColInfo} {
        tmpRowPtrSortingBlock = memoryManager.allocateBlock(
            sizeof(uint8_t*) * (SORT_BLOCK_SIZE / orderByKeyEncoder.getKeyBlockEntrySizeInBytes()));
    }

    void sortAllKeyBlocks();

private:
    void sortSingleKeyBlock(const KeyBlock& keyBlock);

    void solveStringTies(TieRange& keyBlockTie, uint8_t* keyBlockPtr, queue<TieRange>& ties,
        bool isAscOrder, uint64_t fieldOffsetInRowCollection);

    vector<TieRange> findTies(uint8_t* keyBlockPtr, uint64_t numRowsToFindTies,
        uint64_t numBytesToSort, uint64_t baseRowIdx);

    void radixSort(uint8_t* keyBlockPtr, uint64_t numRowsToSort, uint64_t numBytesSorted,
        uint64_t numBytesToSort);

private:
    unique_ptr<MemoryBlock> tmpKeyBlock;
    unique_ptr<MemoryBlock> tmpRowPtrSortingBlock;
    OrderByKeyEncoder& orderByKeyEncoder;
    // rowCollection stores all columns in the rows that will be sorted, including the order by key
    // columns. RadixSort uses rowCollection to access the full contents of the string key columns
    // when resolving ties.
    RowCollection& rowCollection;
    vector<StrKeyColInfo> strKeyColInfo;
};

} // namespace processor
} // namespace graphflow
