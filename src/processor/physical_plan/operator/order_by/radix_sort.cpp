#include "src/processor/include/physical_plan/operator/order_by/radix_sort.h"

#include <algorithm>

using namespace graphflow::common;
namespace graphflow {
namespace processor {

void RadixSort::radixSort(uint8_t* keyBlockPtr, uint64_t numRowsToSort, uint64_t numBytesSorted,
    uint64_t numBytesToSort) {
    // we use radixSortLSD which sorts from the least significant byte to the most significant byte
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
            sortBytePtr += entrySizeInBytes;
        }
        auto maxCounter = count[0];
        for (auto val = 1ul; val < countingArraySize; val++) {
            maxCounter = max(count[val], maxCounter);
            count[val] = count[val] + count[val - 1];
        }
        // if all bytes have the same value (tie), continue on the next byte
        if (maxCounter == numRowsToSort) {
            continue;
        }
        // reorder the data based on the count array
        auto sourceRowPtr = sourcePtr + (numRowsToSort - 1) * entrySizeInBytes;
        for (auto j = 0ul; j < numRowsToSort; j++) {
            auto targetRowNum = --count[*(sourceRowPtr + numBytesToSort - curByteIdx)];
            memcpy(targetPtr + targetRowNum * entrySizeInBytes, sourceRowPtr, entrySizeInBytes);
            sourceRowPtr -= entrySizeInBytes;
        }
        isInTmpBlock = !isInTmpBlock;
    }
    // if the data is in the tmp block, copy the data from tmp block back
    if (isInTmpBlock) {
        memcpy(keyBlockPtr, tmpKeyBlockPtr, numRowsToSort * entrySizeInBytes);
    }
}

vector<TieRange> RadixSort::findTies(uint8_t* keyBlockPtr, uint64_t numRowsToFindTies,
    uint64_t numBytesToSort, uint64_t baseRowIdx) {
    vector<TieRange> newTiesInKeyBlock;
    for (auto i = 0u; i < numRowsToFindTies - 1; i++) {
        auto j = i + 1;
        for (; j < numRowsToFindTies; j++) {
            if (memcmp(keyBlockPtr + i * entrySizeInBytes, keyBlockPtr + j * entrySizeInBytes,
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

void RadixSort::solveStringTies(TieRange& keyBlockTie, uint8_t* keyBlockPtr,
    shared_ptr<ValueVector>& strValueVector, queue<TieRange>& ties, bool isAscOrder) {
    auto tmpRowPtrSortingBlockPtr = (uint8_t**)tmpRowPtrSortingBlock->data;
    for (auto i = 0ul; i < keyBlockTie.getNumRows(); i++) {
        tmpRowPtrSortingBlockPtr[i] = keyBlockPtr + entrySizeInBytes * i;
    }

    sort(tmpRowPtrSortingBlockPtr, tmpRowPtrSortingBlockPtr + keyBlockTie.getNumRows(),
        [this, strValueVector, isAscOrder](
            const uint8_t* leftPtr, const uint8_t* rightPtr) -> bool {
            const auto leftStrIdx = *((uint64_t*)(leftPtr + entrySizeInBytes - sizeof(uint64_t)));
            const auto rightStrIdx = *((uint64_t*)(rightPtr + entrySizeInBytes - sizeof(uint64_t)));
            const auto leftStrVal = ((gf_string_t*)strValueVector->values)[leftStrIdx];
            const auto rightStrVal = ((gf_string_t*)strValueVector->values)[rightStrIdx];
            return isAscOrder ? leftStrVal.getAsString() < rightStrVal.getAsString() :
                                leftStrVal.getAsString() > rightStrVal.getAsString();
        });

    // reorder the keyBlock based on the quick sort result
    auto tmpKeyBlockPtr = tmpKeyBlock->data;
    for (auto i = 0ul; i < keyBlockTie.getNumRows(); i++) {
        memcpy(tmpKeyBlockPtr, tmpRowPtrSortingBlockPtr[i], entrySizeInBytes);
        tmpKeyBlockPtr += entrySizeInBytes;
    }
    memcpy(keyBlockPtr, tmpKeyBlock->data, keyBlockTie.getNumRows() * entrySizeInBytes);

    // some ties can't be solved in quicksort, just add them to ties
    auto strValues = (gf_string_t*)strValueVector->values;
    for (auto i = keyBlockTie.startingRowIdx; i < keyBlockTie.endingRowIdx; i++) {
        auto strValAtIndexI =
            strValues[*((uint64_t*)(keyBlockPtr +
                                    entrySizeInBytes * (i + 1 - keyBlockTie.startingRowIdx) -
                                    sizeof(uint64_t)))]
                .getAsString();
        auto j = i + 1;
        for (; j <= keyBlockTie.endingRowIdx; j++) {
            auto strValAtIndexJ =
                strValues[*((uint64_t*)(keyBlockPtr +
                                        entrySizeInBytes * (j + 1 - keyBlockTie.startingRowIdx) -
                                        sizeof(uint64_t)))]
                    .getAsString();
            if (strValAtIndexI != strValAtIndexJ) {
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

void RadixSort::sortKeyBlock(const MemoryBlock& keyBlock, uint64_t numRowsInKeyBlock) {
    auto numBytesToSort = 0ul;
    auto numBytesSorted = 0ul;
    queue<TieRange> ties;
    // We need to sort the whole keyBlock for the first radix sort, so just mark all rows as a tie
    ties.push(TieRange{0, numRowsInKeyBlock - 1});
    for (auto i = 0u; i < orderByVectors.size(); i++) {
        numBytesToSort += OrderByKeyEncoder::getEncodingSize(orderByVectors[i]->dataType);
        // only do radix sort until we meet a column with variable length datatype(STRING)
        // or this is the last column
        if (orderByVectors[i]->dataType != STRING && i < orderByVectors.size() - 1) {
            continue;
        }
        const auto numOfTies = ties.size();
        for (auto j = 0u; j < numOfTies; j++) {
            auto keyBlockTie = ties.front();
            ties.pop();
            radixSort(keyBlock.data + keyBlockTie.startingRowIdx * entrySizeInBytes,
                keyBlockTie.getNumRows(), numBytesSorted, numBytesToSort);
            // if the current column is a string column, we can try to solve the tie by comparing
            // the overflow ptr
            if (orderByVectors[i]->dataType == STRING) {
                auto newTiesInKeyBlock = findTies(
                    keyBlock.data + keyBlockTie.startingRowIdx * entrySizeInBytes + numBytesSorted,
                    keyBlockTie.getNumRows(), numBytesToSort, keyBlockTie.startingRowIdx);
                for (auto& newTieInKeyBlock : newTiesInKeyBlock) {
                    solveStringTies(newTieInKeyBlock,
                        keyBlock.data + newTieInKeyBlock.startingRowIdx * entrySizeInBytes,
                        orderByVectors[i], ties, isAscOrder[i]);
                }
            }
        }
        if (ties.empty()) {
            return;
        }
        numBytesSorted += numBytesToSort;
        numBytesToSort = 0;
    }
}

} // namespace processor
} // namespace graphflow
