#include "src/processor/include/physical_plan/operator/order_by/key_block_merger.h"

#include "src/common/include/operations/comparison_operations.h"

using namespace graphflow::processor;
using namespace graphflow::common::operation;

namespace graphflow {
namespace processor {

uint64_t KeyBlockMergeTask::findRightKeyBlockIdx(uint8_t* leftEndRowBuffer) {
    // Find a row in the right memory block such that:
    // 1. The value of the current row is smaller than the value in leftEndRow.
    // 2. Either the value of next row is larger than the value in leftEndRow or
    // the current row is the last row in the right memory block.
    int64_t startIdx = rightKeyBlockNextIdx;
    int64_t endIdx = rightKeyBlock->numEntriesInMemBlock - 1;

    while (startIdx <= endIdx) {
        uint64_t curRowIdx = (startIdx + endIdx) / 2;
        uint8_t* curRowBuffer = rightKeyBlock->getMemBlockData() +
                                curRowIdx * keyBlockMerger.getKeyBlockEntrySizeInBytes();
        uint8_t* nextRowBuffer = curRowBuffer + keyBlockMerger.getKeyBlockEntrySizeInBytes();

        if (keyBlockMerger.compareRowBuffer(leftEndRowBuffer, curRowBuffer)) {
            if (curRowIdx == rightKeyBlock->numEntriesInMemBlock - 1 ||
                !keyBlockMerger.compareRowBuffer(leftEndRowBuffer, nextRowBuffer)) {
                // If the current row is the last row or the value of next row is larger than
                // the value of leftEndRow, return the curRowIdx.
                return curRowIdx;
            } else {
                startIdx = curRowIdx + 1;
            }
        } else {
            endIdx = curRowIdx - 1;
        }
    }
    // If such row doesn't exist, return -1.
    return -1;
}

unique_ptr<KeyBlockMergeMorsel> KeyBlockMergeTask::getMorsel() {
    // We grab a batch of rows from the left memory block, then do a binary search on the
    // right memory block to find the range of rows to merge.
    activeMorsels++;
    if (rightKeyBlockNextIdx >= rightKeyBlock->numEntriesInMemBlock) {
        // If there is no more rows left in the right key block,
        // just append all rows in the left key block to the result key block.
        auto keyBlockMergeMorsel = make_unique<KeyBlockMergeMorsel>(leftKeyBlockNextIdx,
            leftKeyBlock->numEntriesInMemBlock, rightKeyBlock->numEntriesInMemBlock,
            rightKeyBlock->numEntriesInMemBlock);
        leftKeyBlockNextIdx = leftKeyBlock->numEntriesInMemBlock;
        return keyBlockMergeMorsel;
    }

    auto leftKeyBlockStartIdx = leftKeyBlockNextIdx;
    leftKeyBlockNextIdx += batch_size;

    if (leftKeyBlockNextIdx >= leftKeyBlock->numEntriesInMemBlock) {
        // This is the last batch of rows in the left key block to merge, so just merge it with
        // remaining rows of the right key block.
        auto keyBlockMergeMorsel = make_unique<KeyBlockMergeMorsel>(leftKeyBlockStartIdx,
            min(leftKeyBlockNextIdx, leftKeyBlock->numEntriesInMemBlock), rightKeyBlockNextIdx,
            rightKeyBlock->numEntriesInMemBlock);
        rightKeyBlockNextIdx = rightKeyBlock->numEntriesInMemBlock;
        return keyBlockMergeMorsel;
    } else {
        // Conduct a binary search to find the ending index in the right memory block.
        auto leftEndIdxBuffer =
            leftKeyBlock->getMemBlockData() +
            (leftKeyBlockNextIdx - 1) * keyBlockMerger.getKeyBlockEntrySizeInBytes();
        auto rightEndIdx = findRightKeyBlockIdx(leftEndIdxBuffer);

        auto keyBlockMergeMorsel = make_unique<KeyBlockMergeMorsel>(leftKeyBlockStartIdx,
            min(leftKeyBlockNextIdx, leftKeyBlock->numEntriesInMemBlock), rightKeyBlockNextIdx,
            rightEndIdx == -1 ? rightKeyBlockNextIdx : ++rightEndIdx);

        if (rightEndIdx != -1) {
            rightKeyBlockNextIdx = rightEndIdx;
        }
        return keyBlockMergeMorsel;
    }
}

void KeyBlockMerger::mergeKeyBlocks(KeyBlockMergeMorsel& keyBlockMergeMorsel) {
    auto leftKeyBlockIdx = keyBlockMergeMorsel.leftKeyBlockStartIdx;
    auto rightKeyBlockIdx = keyBlockMergeMorsel.rightKeyBlockStartIdx;
    auto resultKeyBlockIdx = leftKeyBlockIdx + rightKeyBlockIdx;
    auto leftKeyBlockBuffer =
        keyBlockMergeMorsel.getLeftKeyBlockData() + leftKeyBlockIdx * keyBlockEntrySizeInBytes;
    auto rightKeyBlockBuffer =
        keyBlockMergeMorsel.getRightKeyBlockData() + rightKeyBlockIdx * keyBlockEntrySizeInBytes;
    auto resultKeyBlockBuffer =
        keyBlockMergeMorsel.getResultKeyBlockData() + resultKeyBlockIdx * keyBlockEntrySizeInBytes;

    while (leftKeyBlockIdx < keyBlockMergeMorsel.leftKeyBlockEndIdx &&
           rightKeyBlockIdx < keyBlockMergeMorsel.rightKeyBlockEndIdx) {
        if (compareRowBuffer(leftKeyBlockBuffer, rightKeyBlockBuffer)) {
            memcpy(resultKeyBlockBuffer, rightKeyBlockBuffer, keyBlockEntrySizeInBytes);
            rightKeyBlockBuffer += keyBlockEntrySizeInBytes;
            rightKeyBlockIdx++;
        } else {
            memcpy(resultKeyBlockBuffer, leftKeyBlockBuffer, keyBlockEntrySizeInBytes);
            leftKeyBlockBuffer += keyBlockEntrySizeInBytes;
            leftKeyBlockIdx++;
        }
        resultKeyBlockBuffer += keyBlockEntrySizeInBytes;
    }

    // If there are still unmerged rows in the left or right memBlock, just append them to the
    // result memBlock.
    if (leftKeyBlockIdx < keyBlockMergeMorsel.leftKeyBlockEndIdx) {
        memcpy(resultKeyBlockBuffer, leftKeyBlockBuffer,
            keyBlockEntrySizeInBytes * (keyBlockMergeMorsel.leftKeyBlockEndIdx - leftKeyBlockIdx));
    } else if (rightKeyBlockIdx < keyBlockMergeMorsel.rightKeyBlockEndIdx) {
        memcpy(resultKeyBlockBuffer, rightKeyBlockBuffer,
            keyBlockEntrySizeInBytes *
                (keyBlockMergeMorsel.rightKeyBlockEndIdx - rightKeyBlockIdx));
    }
}

// This function returns true if the value in the leftRowBuffer is larger than the value in the
// rightRowBuffer.
bool KeyBlockMerger::compareRowBuffer(uint8_t* leftRowBuffer, uint8_t* rightRowBuffer) {
    if (stringAndUnstructuredKeyColInfo.empty()) {
        // If there is no string or unstructured columns in the keys, we just need to do a simple
        // memcmp.
        return memcmp(leftRowBuffer, rightRowBuffer, keyBlockEntrySizeInBytes - sizeof(uint64_t)) >
               0;
    } else {
        // We can't simply use memcmp to compare rows if there are string or unstructured columns.
        // We should only compare the binary strings starting from the last compared string column
        // till the next string column.
        uint64_t lastComparedBytes = 0;
        for (auto& stringAndUnstructuredKeyInfo : stringAndUnstructuredKeyColInfo) {
            auto result =
                memcmp(leftRowBuffer + lastComparedBytes, rightRowBuffer + lastComparedBytes,
                    stringAndUnstructuredKeyInfo.colOffsetInEncodedKeyBlock - lastComparedBytes +
                        stringAndUnstructuredKeyInfo.getEncodingSize());
            // If both sides are nulls, we can just continue to check the next string or
            // unstructured column.
            if (OrderByKeyEncoder::isNullVal(
                    leftRowBuffer, stringAndUnstructuredKeyInfo.isAscOrder) &&
                OrderByKeyEncoder::isNullVal(
                    rightRowBuffer, stringAndUnstructuredKeyInfo.isAscOrder)) {
                lastComparedBytes = stringAndUnstructuredKeyInfo.colOffsetInEncodedKeyBlock +
                                    stringAndUnstructuredKeyInfo.getEncodingSize();
                continue;
            }
            // If there is a tie, we need to compare the overflow ptr of strings or the actual
            // unstructured values.
            if (result == 0) {
                auto leftBuffer = getValPtrFromRowCollection(
                    leftRowBuffer, stringAndUnstructuredKeyInfo.colOffsetInRowCollection);
                auto rightBuffer = getValPtrFromRowCollection(
                    rightRowBuffer, stringAndUnstructuredKeyInfo.colOffsetInRowCollection);
                uint8_t result;
                if (stringAndUnstructuredKeyInfo.isStrCol) {
                    Equals::operation<gf_string_t, gf_string_t>(*((gf_string_t*)leftBuffer),
                        *((gf_string_t*)rightBuffer), result, false, false);
                } else {
                    Equals::operation<Value, Value>(
                        *((Value*)leftBuffer), *((Value*)rightBuffer), result, false, false);
                }
                if (result) {
                    // If the tie can't be solved, we need to check the next string or unstructured
                    // column.
                    lastComparedBytes = stringAndUnstructuredKeyInfo.colOffsetInEncodedKeyBlock +
                                        stringAndUnstructuredKeyInfo.getEncodingSize();
                    continue;
                }
                if (stringAndUnstructuredKeyInfo.isStrCol) {
                    GreaterThan::operation<gf_string_t, gf_string_t>(*((gf_string_t*)leftBuffer),
                        *((gf_string_t*)rightBuffer), result, false, false);
                } else {
                    GreaterThan::operation<Value, Value>(
                        *((Value*)leftBuffer), *((Value*)rightBuffer), result, false, false);
                }
                return stringAndUnstructuredKeyInfo.isAscOrder == result;
            }
            return result > 0;
        }
        // The string or unstructured tie can't be solved, just add the row in the leftMemBlock to
        // resultMemBlock.
        return false;
    }
}

unique_ptr<KeyBlockMergeMorsel> KeyBlockMergeTaskDispatcher::getMorsel() {
    if (isDoneMerge()) {
        return nullptr;
    }
    lock_guard<mutex> keyBlockMergeDispatcherLock{mtx};

    if (!activeKeyBlockMergeTasks.empty() && activeKeyBlockMergeTasks.back()->hasMorselLeft()) {
        // If there are morsels left in the lastMergeTask, just give it to the caller.
        auto morsel = activeKeyBlockMergeTasks.back()->getMorsel();
        morsel->keyBlockMergeTask = activeKeyBlockMergeTasks.back();
        return morsel;
    } else if (sortedKeyBlocks->size() > 1) {
        // If there are no morsels left in the lastMergeTask, we just create a new merge task.
        auto leftKeyBlock = sortedKeyBlocks->front();
        sortedKeyBlocks->pop();
        auto rightKeyBlock = sortedKeyBlocks->front();
        sortedKeyBlocks->pop();
        auto resultKeyBlock = make_shared<KeyBlock>(memoryManager->allocateBlock(
            leftKeyBlock->getKeyBlockSize() + rightKeyBlock->getKeyBlockSize()));
        resultKeyBlock->numEntriesInMemBlock =
            leftKeyBlock->numEntriesInMemBlock + rightKeyBlock->numEntriesInMemBlock;
        auto newMergeTask = make_shared<KeyBlockMergeTask>(
            leftKeyBlock, rightKeyBlock, resultKeyBlock, *keyBlockMerger);
        activeKeyBlockMergeTasks.emplace_back(newMergeTask);
        auto morsel = newMergeTask->getMorsel();
        morsel->keyBlockMergeTask = newMergeTask;
        return morsel;
    } else {
        // There is no morsel can be given at this time, just wait for the ongoing merge
        // task to finish.
        return nullptr;
    }
}

void KeyBlockMergeTaskDispatcher::doneMorsel(unique_ptr<KeyBlockMergeMorsel> morsel) {
    lock_guard<mutex> keyBlockMergeDispatcherLock{mtx};
    // If there is no active and morsels left tin the keyBlockMergeTask, just remove it from
    // the active keyBlockMergeTask and add the result key block to the sortedKeyBlocks queue.
    if ((--morsel->keyBlockMergeTask->activeMorsels) == 0 &&
        !morsel->keyBlockMergeTask->hasMorselLeft()) {
        erase(activeKeyBlockMergeTasks, morsel->keyBlockMergeTask);
        sortedKeyBlocks->emplace(morsel->keyBlockMergeTask->resultKeyBlock);
    }
}

} // namespace processor
} // namespace graphflow
