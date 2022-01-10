#include "src/processor/include/physical_plan/operator/order_by/radix_sort.h"

#include <algorithm>

#include "src/common/include/operations/comparison_operations.h"

using namespace graphflow::common::operation;

namespace graphflow {
namespace processor {

void RadixSort::radixSort(uint8_t* keyBlockPtr, uint64_t numRowsToSort, uint64_t numBytesSorted,
    uint64_t numBytesToSort) {
    // We use radixSortLSD which sorts from the least significant byte to the most significant byte.
    auto tmpKeyBlockPtr = tmpKeyBlock->data;
    keyBlockPtr += numBytesSorted;
    tmpKeyBlockPtr += numBytesSorted;
    constexpr uint16_t countingArraySize = 256;
    uint64_t count[countingArraySize];
    auto isInTmpBlock = false;
    for (auto curByteIdx = 1ul; curByteIdx <= numBytesToSort; curByteIdx++) {
        memset(count, 0, countingArraySize * sizeof(uint64_t));
        auto sourcePtr = isInTmpBlock ? tmpKeyBlockPtr : keyBlockPtr;
        auto targetPtr = isInTmpBlock ? keyBlockPtr : tmpKeyBlockPtr;
        auto sortBytePtr = sourcePtr + numBytesToSort - curByteIdx;
        // counting sort
        for (auto j = 0ul; j < numRowsToSort; j++) {
            count[*sortBytePtr]++;
            sortBytePtr += orderByKeyEncoder.getKeyBlockEntrySizeInBytes();
        }
        auto maxCounter = count[0];
        for (auto val = 1ul; val < countingArraySize; val++) {
            maxCounter = max(count[val], maxCounter);
            count[val] = count[val] + count[val - 1];
        }
        // If all bytes have the same value (tie), continue on the next byte.
        if (maxCounter == numRowsToSort) {
            continue;
        }
        // Reorder the data based on the count array.
        auto sourceRowPtr =
            sourcePtr + (numRowsToSort - 1) * orderByKeyEncoder.getKeyBlockEntrySizeInBytes();
        for (auto j = 0ul; j < numRowsToSort; j++) {
            auto targetRowNum = --count[*(sourceRowPtr + numBytesToSort - curByteIdx)];
            memcpy(targetPtr + targetRowNum * orderByKeyEncoder.getKeyBlockEntrySizeInBytes(),
                sourceRowPtr, orderByKeyEncoder.getKeyBlockEntrySizeInBytes());
            sourceRowPtr -= orderByKeyEncoder.getKeyBlockEntrySizeInBytes();
        }
        isInTmpBlock = !isInTmpBlock;
    }
    // If the data is in the tmp block, copy the data from tmp block back.
    if (isInTmpBlock) {
        memcpy(keyBlockPtr, tmpKeyBlockPtr,
            numRowsToSort * orderByKeyEncoder.getKeyBlockEntrySizeInBytes());
    }
}

vector<TieRange> RadixSort::findTies(uint8_t* keyBlockPtr, uint64_t numRowsToFindTies,
    uint64_t numBytesToSort, uint64_t baseRowIdx) {
    vector<TieRange> newTiesInKeyBlock;
    for (auto i = 0u; i < numRowsToFindTies - 1; i++) {
        auto j = i + 1;
        for (; j < numRowsToFindTies; j++) {
            if (memcmp(keyBlockPtr + i * orderByKeyEncoder.getKeyBlockEntrySizeInBytes(),
                    keyBlockPtr + j * orderByKeyEncoder.getKeyBlockEntrySizeInBytes(),
                    numBytesToSort) != 0) {
                break;
            }
        }
        j--;
        if (i != j) {
            newTiesInKeyBlock.emplace_back(TieRange(i + baseRowIdx, j + baseRowIdx));
        }
        i = j;
    }
    return newTiesInKeyBlock;
}

void RadixSort::solveStringAndUnstructuredTies(TieRange& keyBlockTie, uint8_t* keyBlockPtr,
    queue<TieRange>& ties, StringAndUnstructuredKeyColInfo& stringAndUnstructuredKeyColInfo) {
    auto tmpRowPtrSortingBlockPtr = (uint8_t**)tmpRowPtrSortingBlock->data;
    for (auto i = 0ul; i < keyBlockTie.getNumRows(); i++) {
        tmpRowPtrSortingBlockPtr[i] =
            keyBlockPtr + orderByKeyEncoder.getKeyBlockEntrySizeInBytes() * i;
    }

    sort(tmpRowPtrSortingBlockPtr, tmpRowPtrSortingBlockPtr + keyBlockTie.getNumRows(),
        [this, stringAndUnstructuredKeyColInfo](
            const uint8_t* leftPtr, const uint8_t* rightPtr) -> bool {
            // Handle null value comparison.
            if (OrderByKeyEncoder::isNullVal(
                    rightPtr + stringAndUnstructuredKeyColInfo.colOffsetInEncodedKeyBlock,
                    stringAndUnstructuredKeyColInfo.isAscOrder)) {
                return stringAndUnstructuredKeyColInfo.isAscOrder;
            } else if (OrderByKeyEncoder::isNullVal(
                           leftPtr + stringAndUnstructuredKeyColInfo.colOffsetInEncodedKeyBlock,
                           stringAndUnstructuredKeyColInfo.isAscOrder)) {
                return !stringAndUnstructuredKeyColInfo.isAscOrder;
            }

            const auto leftStrRowIdx = OrderByKeyEncoder::getEncodedRowIdx(
                leftPtr + orderByKeyEncoder.getKeyBlockEntrySizeInBytes() - sizeof(uint64_t));
            const auto rightStrRowIdx = OrderByKeyEncoder::getEncodedRowIdx(
                rightPtr + orderByKeyEncoder.getKeyBlockEntrySizeInBytes() - sizeof(uint64_t));
            const auto leftVal =
                (uint8_t*)(this->rowCollection.getRow(leftStrRowIdx) +
                           stringAndUnstructuredKeyColInfo.colOffsetInRowCollection);
            const auto rightVal =
                (uint8_t*)(this->rowCollection.getRow(rightStrRowIdx) +
                           stringAndUnstructuredKeyColInfo.colOffsetInRowCollection);
            uint8_t result;
            if (stringAndUnstructuredKeyColInfo.isStrCol) {
                LessThan::operation<gf_string_t, gf_string_t>(
                    *((gf_string_t*)leftVal), *((gf_string_t*)rightVal), result, false, false);
                return stringAndUnstructuredKeyColInfo.isAscOrder == result;
            } else {
                // The comparison function does the type checking for the unstructured values.
                // If there is a type mismatch, the comparison function will throw an exception.
                // Note: we may loose precision if we compare DOUBLE and INT64
                // For example: DOUBLE: a = 2^57, INT64: b = 2^57 + 3.
                // Although a < b, the LessThan function may still output false.
                LessThan::operation<Value, Value>(
                    *((Value*)leftVal), *((Value*)rightVal), result, false, false);
                return stringAndUnstructuredKeyColInfo.isAscOrder == result;
            }
        });

    // Reorder the keyBlock based on the quick sort result.
    auto tmpKeyBlockPtr = tmpKeyBlock->data;
    for (auto i = 0ul; i < keyBlockTie.getNumRows(); i++) {
        memcpy(tmpKeyBlockPtr, tmpRowPtrSortingBlockPtr[i],
            orderByKeyEncoder.getKeyBlockEntrySizeInBytes());
        tmpKeyBlockPtr += orderByKeyEncoder.getKeyBlockEntrySizeInBytes();
    }
    memcpy(keyBlockPtr, tmpKeyBlock->data,
        keyBlockTie.getNumRows() * orderByKeyEncoder.getKeyBlockEntrySizeInBytes());

    // Some ties can't be solved in quicksort, just add them to ties.
    for (auto i = keyBlockTie.startingRowIdx; i < keyBlockTie.endingRowIdx; i++) {
        auto rowIdxAtIdxI = OrderByKeyEncoder::getEncodedRowIdx(
            keyBlockPtr +
            orderByKeyEncoder.getKeyBlockEntrySizeInBytes() * (i + 1 - keyBlockTie.startingRowIdx) -
            sizeof(uint64_t));
        auto valAtIdxI = (uint8_t*)(rowCollection.getRow(rowIdxAtIdxI) +
                                    stringAndUnstructuredKeyColInfo.colOffsetInRowCollection);
        bool isValAtIdxINull = OrderByKeyEncoder::isNullVal(
            keyBlockPtr +
                orderByKeyEncoder.getKeyBlockEntrySizeInBytes() * (i - keyBlockTie.startingRowIdx) +
                stringAndUnstructuredKeyColInfo.colOffsetInEncodedKeyBlock,
            stringAndUnstructuredKeyColInfo.isAscOrder);

        auto j = i + 1;
        for (; j <= keyBlockTie.endingRowIdx; j++) {
            auto rowIdxAtIdxJ = OrderByKeyEncoder::getEncodedRowIdx(
                keyBlockPtr +
                orderByKeyEncoder.getKeyBlockEntrySizeInBytes() *
                    (j + 1 - keyBlockTie.startingRowIdx) -
                sizeof(uint64_t));
            auto valAtIdxJ = (uint8_t*)(rowCollection.getRow(rowIdxAtIdxJ) +
                                        stringAndUnstructuredKeyColInfo.colOffsetInRowCollection);
            bool isValAtIdxJNull = OrderByKeyEncoder::isNullVal(
                keyBlockPtr +
                    orderByKeyEncoder.getKeyBlockEntrySizeInBytes() *
                        (j - keyBlockTie.startingRowIdx) +
                    stringAndUnstructuredKeyColInfo.colOffsetInEncodedKeyBlock,
                stringAndUnstructuredKeyColInfo.isAscOrder);

            if (isValAtIdxINull && isValAtIdxJNull) {
                // If the left value and the right value are nulls, we can just continue on the next
                // row.
                continue;
            } else if (isValAtIdxINull || isValAtIdxJNull) {
                // If only one value is null, we can just conclude that those two values are not
                // equal.
                break;
            }

            uint8_t result;
            if (stringAndUnstructuredKeyColInfo.isStrCol) {
                NotEquals::operation<gf_string_t, gf_string_t>(
                    *((gf_string_t*)valAtIdxI), *((gf_string_t*)valAtIdxJ), result, false, false);
            } else {
                NotEquals::operation<Value, Value>(
                    *((Value*)valAtIdxI), *((Value*)valAtIdxJ), result, false, false);
            }
            if (result) {
                break;
            }
        }
        j--;
        if (i != j) {
            ties.push(TieRange(i, j));
        }
        i = j;
    }
}

void RadixSort::sortSingleKeyBlock(const KeyBlock& keyBlock) {
    auto numBytesSorted = 0ul;
    auto numRowsInKeyBlock = keyBlock.numEntriesInMemBlock;
    queue<TieRange> ties;
    // We need to sort the whole keyBlock for the first radix sort, so just mark all rows as a tie.
    ties.push(TieRange{0, numRowsInKeyBlock - 1});
    for (auto i = 0u; i < stringAndUnstructuredKeyColInfo.size(); i++) {
        const auto numBytesToSort = stringAndUnstructuredKeyColInfo[i].colOffsetInEncodedKeyBlock -
                                    numBytesSorted +
                                    stringAndUnstructuredKeyColInfo[i].getEncodingSize();
        const auto numOfTies = ties.size();
        for (auto j = 0u; j < numOfTies; j++) {
            auto keyBlockTie = ties.front();
            ties.pop();
            radixSort(
                keyBlock.getMemBlockData() +
                    keyBlockTie.startingRowIdx * orderByKeyEncoder.getKeyBlockEntrySizeInBytes(),
                keyBlockTie.getNumRows(), numBytesSorted, numBytesToSort);

            auto newTiesInKeyBlock = findTies(
                keyBlock.getMemBlockData() +
                    keyBlockTie.startingRowIdx * orderByKeyEncoder.getKeyBlockEntrySizeInBytes() +
                    numBytesSorted,
                keyBlockTie.getNumRows(), numBytesToSort, keyBlockTie.startingRowIdx);
            for (auto& newTieInKeyBlock : newTiesInKeyBlock) {
                solveStringAndUnstructuredTies(newTieInKeyBlock,
                    keyBlock.getMemBlockData() +
                        newTieInKeyBlock.startingRowIdx *
                            orderByKeyEncoder.getKeyBlockEntrySizeInBytes(),
                    ties, stringAndUnstructuredKeyColInfo[i]);
            }
        }
        if (ties.empty()) {
            return;
        }
        numBytesSorted += numBytesToSort;
    }

    if (numBytesSorted < orderByKeyEncoder.getKeyBlockEntrySizeInBytes()) {
        while (!ties.empty()) {
            auto tie = ties.front();
            ties.pop();
            radixSort(keyBlock.getMemBlockData() +
                          tie.startingRowIdx * orderByKeyEncoder.getKeyBlockEntrySizeInBytes(),
                tie.getNumRows(), numBytesSorted,
                orderByKeyEncoder.getKeyBlockEntrySizeInBytes() - numBytesSorted);
        }
    }
}

} // namespace processor
} // namespace graphflow
