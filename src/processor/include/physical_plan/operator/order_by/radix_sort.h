#pragma once
#include <queue>

#include "src/common/include/configs.h"
#include "src/common/include/vector/value_vector.h"
#include "src/processor/include/physical_plan/operator/order_by/key_block_merger.h"
#include "src/processor/include/physical_plan/operator/order_by/order_by_key_encoder.h"
#include "src/processor/include/physical_plan/result/factorized_table.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

struct TieRange {
public:
    uint64_t startingTupleIdx;
    uint64_t endingTupleIdx;
    inline uint64_t getNumTuples() { return endingTupleIdx - startingTupleIdx + 1; }
    explicit TieRange(uint64_t startingTupleIdx, uint64_t endingTupleIdx)
        : startingTupleIdx{startingTupleIdx}, endingTupleIdx{endingTupleIdx} {}
};

// RadixSort sorts a block of binary strings using the radixSort and quickSort (only for comparing
// string overflow pointers). The algorithm loops through each column of the orderByVectors. If it
// sees a column with string, which is variable length, or unstructured type, which may be variable
// length, it will call radixSort to sort the columns seen so far. If there are tie tuples, it will
// compare the overflow ptr of strings or the actual values of unstructured data. For subsequent
// columns, the algorithm only calls radixSort on tie tuples.
class RadixSort {
public:
    explicit RadixSort(MemoryManager& memoryManager, FactorizedTable& factorizedTable,
        OrderByKeyEncoder& orderByKeyEncoder,
        vector<StringAndUnstructuredKeyColInfo> stringAndUnstructuredKeyColInfo)
        : factorizedTable{factorizedTable}, tmpKeyBlock{memoryManager.allocateBlock(
                                                SORT_BLOCK_SIZE)},
          orderByKeyEncoder{orderByKeyEncoder}, stringAndUnstructuredKeyColInfo{
                                                    stringAndUnstructuredKeyColInfo} {
        tmpTuplePtrSortingBlock = memoryManager.allocateBlock(
            sizeof(uint8_t*) * (SORT_BLOCK_SIZE / orderByKeyEncoder.getKeyBlockEntrySizeInBytes()));
    }

    void sortSingleKeyBlock(const KeyBlock& keyBlock);

private:
    void solveStringAndUnstructuredTies(TieRange& keyBlockTie, uint8_t* keyBlockPtr,
        queue<TieRange>& ties, StringAndUnstructuredKeyColInfo& stringAndUnstructuredKeyColInfo);

    vector<TieRange> findTies(uint8_t* keyBlockPtr, uint64_t numTuplesToFindTies,
        uint64_t numBytesToSort, uint64_t baseTupleIdx);

    void radixSort(uint8_t* keyBlockPtr, uint64_t numTuplesToSort, uint64_t numBytesSorted,
        uint64_t numBytesToSort);

private:
    unique_ptr<MemoryBlock> tmpKeyBlock;
    unique_ptr<MemoryBlock> tmpTuplePtrSortingBlock;
    OrderByKeyEncoder& orderByKeyEncoder;
    // factorizedTable stores all columns in the tuples that will be sorted, including the order by
    // key columns. RadixSort uses factorizedTable to access the full contents of the string and
    // unstructured columns when resolving ties.
    FactorizedTable& factorizedTable;
    vector<StringAndUnstructuredKeyColInfo> stringAndUnstructuredKeyColInfo;
};

} // namespace processor
} // namespace graphflow
