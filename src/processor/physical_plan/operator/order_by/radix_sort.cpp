#include "src/processor/include/physical_plan/operator/order_by/radix_sort.h"

#include <algorithm>

#include "src/function/comparison/operations/include/comparison_operations.h"

using namespace graphflow::function::operation;

namespace graphflow {
namespace processor {

void RadixSort::radixSort(uint8_t* keyBlockPtr, uint64_t numTuplesToSort, uint64_t numBytesSorted,
    uint64_t numBytesToSort) {
    // We use radixSortLSD which sorts from the least significant byte to the most significant byte.
    auto tmpKeyBlockPtr = tmpSortingResultBlock->getData();
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
        for (auto j = 0ul; j < numTuplesToSort; j++) {
            count[*sortBytePtr]++;
            sortBytePtr += orderByKeyEncoder.getNumBytesPerTuple();
        }
        auto maxCounter = count[0];
        for (auto val = 1ul; val < countingArraySize; val++) {
            maxCounter = max(count[val], maxCounter);
            count[val] = count[val] + count[val - 1];
        }
        // If all bytes have the same value (tie), continue on the next byte.
        if (maxCounter == numTuplesToSort) {
            continue;
        }
        // Reorder the data based on the count array.
        auto sourceTuplePtr =
            sourcePtr + (numTuplesToSort - 1) * orderByKeyEncoder.getNumBytesPerTuple();
        for (auto j = 0ul; j < numTuplesToSort; j++) {
            auto targetTupleNum = --count[*(sourceTuplePtr + numBytesToSort - curByteIdx)];
            memcpy(targetPtr + targetTupleNum * orderByKeyEncoder.getNumBytesPerTuple(),
                sourceTuplePtr, orderByKeyEncoder.getNumBytesPerTuple());
            sourceTuplePtr -= orderByKeyEncoder.getNumBytesPerTuple();
        }
        isInTmpBlock = !isInTmpBlock;
    }
    // If the data is in the tmp block, copy the data from tmp block back.
    if (isInTmpBlock) {
        memcpy(
            keyBlockPtr, tmpKeyBlockPtr, numTuplesToSort * orderByKeyEncoder.getNumBytesPerTuple());
    }
}

vector<TieRange> RadixSort::findTies(uint8_t* keyBlockPtr, uint64_t numTuplesToFindTies,
    uint64_t numBytesToSort, uint64_t baseTupleIdx) {
    vector<TieRange> newTiesInKeyBlock;
    for (auto i = 0u; i < numTuplesToFindTies - 1; i++) {
        auto j = i + 1;
        for (; j < numTuplesToFindTies; j++) {
            if (memcmp(keyBlockPtr + i * orderByKeyEncoder.getNumBytesPerTuple(),
                    keyBlockPtr + j * orderByKeyEncoder.getNumBytesPerTuple(),
                    numBytesToSort) != 0) {
                break;
            }
        }
        j--;
        if (i != j) {
            newTiesInKeyBlock.emplace_back(TieRange(i + baseTupleIdx, j + baseTupleIdx));
        }
        i = j;
    }
    return newTiesInKeyBlock;
}

void RadixSort::solveStringAndUnstructuredTies(TieRange& keyBlockTie, uint8_t* keyBlockPtr,
    queue<TieRange>& ties, StringAndUnstructuredKeyColInfo& stringAndUnstructuredKeyColInfo) {
    auto tmpTuplePtrSortingBlockPtr = (uint8_t**)tmpTuplePtrSortingBlock->getData();
    for (auto i = 0ul; i < keyBlockTie.getNumTuples(); i++) {
        tmpTuplePtrSortingBlockPtr[i] = keyBlockPtr + orderByKeyEncoder.getNumBytesPerTuple() * i;
    }

    sort(tmpTuplePtrSortingBlockPtr, tmpTuplePtrSortingBlockPtr + keyBlockTie.getNumTuples(),
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

            const auto leftStrAndUnstrTupleIdx = OrderByKeyEncoder::getEncodedTupleIdx(
                leftPtr + orderByKeyEncoder.getNumBytesPerTuple() - sizeof(uint64_t));
            const auto rightStrTupleIdx = OrderByKeyEncoder::getEncodedTupleIdx(
                rightPtr + orderByKeyEncoder.getNumBytesPerTuple() - sizeof(uint64_t));
            uint8_t result;
            if (stringAndUnstructuredKeyColInfo.isStrCol) {
                LessThan::operation<gf_string_t, gf_string_t>(
                    factorizedTable.getString(leftStrAndUnstrTupleIdx,
                        stringAndUnstructuredKeyColInfo.colIdxInFactorizedTable),
                    factorizedTable.getString(
                        rightStrTupleIdx, stringAndUnstructuredKeyColInfo.colIdxInFactorizedTable),
                    result, false, false);
                return stringAndUnstructuredKeyColInfo.isAscOrder == result;
            } else {
                // The comparison function does the type checking for the unstructured values.
                // If there is a type mismatch, the comparison function will throw an exception.
                // Note: we may loose precision if we compare DOUBLE and INT64
                // For example: DOUBLE: a = 2^57, INT64: b = 2^57 + 3.
                // Although a < b, the LessThan function may still output false.
                LessThan::operation<Value, Value>(
                    factorizedTable.getUnstrValue(leftStrAndUnstrTupleIdx,
                        stringAndUnstructuredKeyColInfo.colIdxInFactorizedTable),
                    factorizedTable.getUnstrValue(
                        rightStrTupleIdx, stringAndUnstructuredKeyColInfo.colIdxInFactorizedTable),
                    result, false, false);
                return stringAndUnstructuredKeyColInfo.isAscOrder == result;
            }
        });

    // Reorder the keyBlock based on the quick sort result.
    auto tmpKeyBlockPtr = tmpSortingResultBlock->getData();
    for (auto i = 0ul; i < keyBlockTie.getNumTuples(); i++) {
        memcpy(
            tmpKeyBlockPtr, tmpTuplePtrSortingBlockPtr[i], orderByKeyEncoder.getNumBytesPerTuple());
        tmpKeyBlockPtr += orderByKeyEncoder.getNumBytesPerTuple();
    }
    memcpy(keyBlockPtr, tmpSortingResultBlock->getData(),
        keyBlockTie.getNumTuples() * orderByKeyEncoder.getNumBytesPerTuple());

    // Some ties can't be solved in quicksort, just add them to ties.
    for (auto i = keyBlockTie.startingTupleIdx; i < keyBlockTie.endingTupleIdx; i++) {
        auto tupleIdxAtIdxI = OrderByKeyEncoder::getEncodedTupleIdx(
            keyBlockPtr +
            orderByKeyEncoder.getNumBytesPerTuple() * (i + 1 - keyBlockTie.startingTupleIdx) -
            sizeof(uint64_t));
        bool isValAtIdxINull = OrderByKeyEncoder::isNullVal(
            keyBlockPtr +
                orderByKeyEncoder.getNumBytesPerTuple() * (i - keyBlockTie.startingTupleIdx) +
                stringAndUnstructuredKeyColInfo.colOffsetInEncodedKeyBlock,
            stringAndUnstructuredKeyColInfo.isAscOrder);

        auto j = i + 1;
        for (; j <= keyBlockTie.endingTupleIdx; j++) {
            auto tupleIdxAtIdxJ = OrderByKeyEncoder::getEncodedTupleIdx(
                keyBlockPtr +
                orderByKeyEncoder.getNumBytesPerTuple() * (j + 1 - keyBlockTie.startingTupleIdx) -
                sizeof(uint64_t));
            bool isValAtIdxJNull = OrderByKeyEncoder::isNullVal(
                keyBlockPtr +
                    orderByKeyEncoder.getNumBytesPerTuple() * (j - keyBlockTie.startingTupleIdx) +
                    stringAndUnstructuredKeyColInfo.colOffsetInEncodedKeyBlock,
                stringAndUnstructuredKeyColInfo.isAscOrder);

            if (isValAtIdxINull && isValAtIdxJNull) {
                // If the left value and the right value are nulls, we can just continue on the next
                // tuple.
                continue;
            } else if (isValAtIdxINull || isValAtIdxJNull) {
                // If only one value is null, we can just conclude that those two values are not
                // equal.
                break;
            }

            uint8_t result;
            if (stringAndUnstructuredKeyColInfo.isStrCol) {
                NotEquals::operation<gf_string_t, gf_string_t>(
                    factorizedTable.getString(
                        tupleIdxAtIdxI, stringAndUnstructuredKeyColInfo.colIdxInFactorizedTable),
                    factorizedTable.getString(
                        tupleIdxAtIdxJ, stringAndUnstructuredKeyColInfo.colIdxInFactorizedTable),
                    result, false, false);
            } else {
                NotEquals::operation<Value, Value>(
                    factorizedTable.getUnstrValue(
                        tupleIdxAtIdxI, stringAndUnstructuredKeyColInfo.colIdxInFactorizedTable),
                    factorizedTable.getUnstrValue(
                        tupleIdxAtIdxJ, stringAndUnstructuredKeyColInfo.colIdxInFactorizedTable),
                    result, false, false);
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

void RadixSort::sortSingleKeyBlock(const DataBlock& keyBlock) {
    auto numBytesSorted = 0ul;
    auto numTuplesInKeyBlock = keyBlock.numTuples;
    queue<TieRange> ties;
    // We need to sort the whole keyBlock for the first radix sort, so just mark all tuples as a
    // tie.
    ties.push(TieRange{0, numTuplesInKeyBlock - 1});
    for (auto i = 0u; i < stringAndUnstructuredKeyColInfo.size(); i++) {
        const auto numBytesToSort = stringAndUnstructuredKeyColInfo[i].colOffsetInEncodedKeyBlock -
                                    numBytesSorted +
                                    stringAndUnstructuredKeyColInfo[i].getEncodingSize();
        const auto numOfTies = ties.size();
        for (auto j = 0u; j < numOfTies; j++) {
            auto keyBlockTie = ties.front();
            ties.pop();
            radixSort(keyBlock.getData() +
                          keyBlockTie.startingTupleIdx * orderByKeyEncoder.getNumBytesPerTuple(),
                keyBlockTie.getNumTuples(), numBytesSorted, numBytesToSort);

            auto newTiesInKeyBlock = findTies(
                keyBlock.getData() +
                    keyBlockTie.startingTupleIdx * orderByKeyEncoder.getNumBytesPerTuple() +
                    numBytesSorted,
                keyBlockTie.getNumTuples(), numBytesToSort, keyBlockTie.startingTupleIdx);
            for (auto& newTieInKeyBlock : newTiesInKeyBlock) {
                solveStringAndUnstructuredTies(newTieInKeyBlock,
                    keyBlock.getData() +
                        newTieInKeyBlock.startingTupleIdx * orderByKeyEncoder.getNumBytesPerTuple(),
                    ties, stringAndUnstructuredKeyColInfo[i]);
            }
        }
        if (ties.empty()) {
            return;
        }
        numBytesSorted += numBytesToSort;
    }

    if (numBytesSorted < orderByKeyEncoder.getNumBytesPerTuple()) {
        while (!ties.empty()) {
            auto tie = ties.front();
            ties.pop();
            radixSort(
                keyBlock.getData() + tie.startingTupleIdx * orderByKeyEncoder.getNumBytesPerTuple(),
                tie.getNumTuples(), numBytesSorted,
                orderByKeyEncoder.getNumBytesPerTuple() - numBytesSorted);
        }
    }
}

} // namespace processor
} // namespace graphflow
