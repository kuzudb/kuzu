#include "src/processor/include/physical_plan/operator/order_by/key_block_merger.h"

#include "src/function/comparison/operations/include/comparison_operations.h"

using namespace graphflow::processor;
using namespace graphflow::function::operation;

namespace graphflow {
namespace processor {

MergedKeyBlocks::MergedKeyBlocks(
    uint64_t numBytesPerTuple, uint64_t numTuples, MemoryManager* memoryManager)
    : numBytesPerTuple{numBytesPerTuple}, numTuples{numTuples} {
    auto numTuplesPerBlock = LARGE_PAGE_SIZE / numBytesPerTuple;
    auto numKeyBlocks = numTuples / numTuplesPerBlock + (numTuples % numTuplesPerBlock ? 1 : 0);
    for (auto i = 0u; i < numKeyBlocks; i++) {
        keyBlocks.emplace_back(make_shared<DataBlock>(memoryManager));
    }
};

// This constructor is used to convert a keyBlock to a MergedKeyBlocks.
MergedKeyBlocks::MergedKeyBlocks(uint64_t numBytesPerTuple, shared_ptr<DataBlock> keyBlock)
    : numBytesPerTuple{numBytesPerTuple}, numTuples{keyBlock->numTuples} {
    keyBlocks.emplace_back(keyBlock);
}

pair<uint64_t, uint64_t> KeyBlockMerger::getEncodedFactorizedTableIdxAndTupleIdx(
    uint8_t* encodedTupleBuffer) const {
    auto encodedTupleInfoBuffer = encodedTupleBuffer + numBytesPerTuple - sizeof(uint64_t);
    auto encodedTupleIdx = OrderByKeyEncoder::getEncodedTupleIdx(encodedTupleInfoBuffer);
    auto encodedFactorizedTableIdx =
        OrderByKeyEncoder::getEncodedFactorizedTableIdx(encodedTupleInfoBuffer);
    return make_pair(encodedFactorizedTableIdx, encodedTupleIdx);
}

uint64_t KeyBlockMergeTask::findRightKeyBlockIdx(uint8_t* leftEndTupleBuffer) {
    // Find a tuple in the right memory block such that:
    // 1. The value of the current tuple is smaller than the value in leftEndTuple.
    // 2. Either the value of next tuple is larger than the value in leftEndTuple or
    // the current tuple is the last tuple in the right memory block.
    int64_t startIdx = rightKeyBlockNextIdx;
    int64_t endIdx = rightKeyBlock->getNumTuples() - 1;

    while (startIdx <= endIdx) {
        uint64_t curTupleIdx = (startIdx + endIdx) / 2;
        uint8_t* curTupleBuffer = rightKeyBlock->getTuple(curTupleIdx);
        uint8_t* nextTupleBuffer = rightKeyBlock->getTuple(curTupleIdx + 1);

        if (keyBlockMerger.compareTupleBuffer(leftEndTupleBuffer, curTupleBuffer)) {
            if (curTupleIdx == rightKeyBlock->getNumTuples() - 1 ||
                !keyBlockMerger.compareTupleBuffer(leftEndTupleBuffer, nextTupleBuffer)) {
                // If the current tuple is the last tuple or the value of next tuple is larger than
                // the value of leftEndTuple, return the curTupleIdx.
                return curTupleIdx;
            } else {
                startIdx = curTupleIdx + 1;
            }
        } else {
            endIdx = curTupleIdx - 1;
        }
    }
    // If such tuple doesn't exist, return -1.
    return -1;
}

unique_ptr<KeyBlockMergeMorsel> KeyBlockMergeTask::getMorsel() {
    // We grab a batch of tuples from the left memory block, then do a binary search on the
    // right memory block to find the range of tuples to merge.
    activeMorsels++;
    if (rightKeyBlockNextIdx >= rightKeyBlock->getNumTuples()) {
        // If there is no more tuples left in the right key block,
        // just append all tuples in the left key block to the result key block.
        auto keyBlockMergeMorsel =
            make_unique<KeyBlockMergeMorsel>(leftKeyBlockNextIdx, leftKeyBlock->getNumTuples(),
                rightKeyBlock->getNumTuples(), rightKeyBlock->getNumTuples());
        leftKeyBlockNextIdx = leftKeyBlock->getNumTuples();
        return keyBlockMergeMorsel;
    }

    auto leftKeyBlockStartIdx = leftKeyBlockNextIdx;
    leftKeyBlockNextIdx += batch_size;

    if (leftKeyBlockNextIdx >= leftKeyBlock->getNumTuples()) {
        // This is the last batch of tuples in the left key block to merge, so just merge it with
        // remaining tuples of the right key block.
        auto keyBlockMergeMorsel = make_unique<KeyBlockMergeMorsel>(leftKeyBlockStartIdx,
            min(leftKeyBlockNextIdx, leftKeyBlock->getNumTuples()), rightKeyBlockNextIdx,
            rightKeyBlock->getNumTuples());
        rightKeyBlockNextIdx = rightKeyBlock->getNumTuples();
        return keyBlockMergeMorsel;
    } else {
        // Conduct a binary search to find the ending index in the right memory block.
        auto leftEndIdxBuffer = leftKeyBlock->getTuple(leftKeyBlockNextIdx - 1);
        auto rightEndIdx = findRightKeyBlockIdx(leftEndIdxBuffer);

        auto keyBlockMergeMorsel = make_unique<KeyBlockMergeMorsel>(leftKeyBlockStartIdx,
            min(leftKeyBlockNextIdx, leftKeyBlock->getNumTuples()), rightKeyBlockNextIdx,
            rightEndIdx == -1 ? rightKeyBlockNextIdx : ++rightEndIdx);

        if (rightEndIdx != -1) {
            rightKeyBlockNextIdx = rightEndIdx;
        }
        return keyBlockMergeMorsel;
    }
}

void KeyBlockMerger::mergeKeyBlocks(KeyBlockMergeMorsel& keyBlockMergeMorsel) const {
    auto leftKeyBlockIdx = keyBlockMergeMorsel.leftKeyBlockStartIdx;
    auto rightKeyBlockIdx = keyBlockMergeMorsel.rightKeyBlockStartIdx;
    auto leftKeyBlock = keyBlockMergeMorsel.keyBlockMergeTask->leftKeyBlock;
    auto rightKeyBlock = keyBlockMergeMorsel.keyBlockMergeTask->rightKeyBlock;
    auto resultKeyBlock = keyBlockMergeMorsel.keyBlockMergeTask->resultKeyBlock;
    auto leftKeyBlockBuffer = leftKeyBlock->getTuple(leftKeyBlockIdx);
    auto rightKeyBlockBuffer = rightKeyBlock->getTuple(rightKeyBlockIdx);
    auto resultKeyBlockBuffer = resultKeyBlock->getTuple(leftKeyBlockIdx + rightKeyBlockIdx);

    while (leftKeyBlockIdx < keyBlockMergeMorsel.leftKeyBlockEndIdx &&
           rightKeyBlockIdx < keyBlockMergeMorsel.rightKeyBlockEndIdx) {
        if (compareTupleBuffer(leftKeyBlockBuffer, rightKeyBlockBuffer)) {
            memcpy(resultKeyBlockBuffer, rightKeyBlockBuffer, numBytesPerTuple);
            rightKeyBlockIdx++;
            rightKeyBlockBuffer = rightKeyBlock->getTuple(rightKeyBlockIdx);
        } else {
            memcpy(resultKeyBlockBuffer, leftKeyBlockBuffer, numBytesPerTuple);
            leftKeyBlockIdx++;
            leftKeyBlockBuffer = leftKeyBlock->getTuple(leftKeyBlockIdx);
        }
        resultKeyBlockBuffer = resultKeyBlock->getTuple(leftKeyBlockIdx + rightKeyBlockIdx);
    }

    // If there are still unmerged tuples in the left or right memBlock, just append them to the
    // result memBlock.
    while (leftKeyBlockIdx < keyBlockMergeMorsel.leftKeyBlockEndIdx) {
        memcpy(resultKeyBlockBuffer, leftKeyBlockBuffer, numBytesPerTuple);
        leftKeyBlockIdx++;
        leftKeyBlockBuffer = leftKeyBlock->getTuple(leftKeyBlockIdx);
        resultKeyBlockBuffer = resultKeyBlock->getTuple(leftKeyBlockIdx + rightKeyBlockIdx);
    }

    while (rightKeyBlockIdx < keyBlockMergeMorsel.rightKeyBlockEndIdx) {
        memcpy(resultKeyBlockBuffer, rightKeyBlockBuffer, numBytesPerTuple);
        rightKeyBlockIdx++;
        rightKeyBlockBuffer = rightKeyBlock->getTuple(rightKeyBlockIdx);
        resultKeyBlockBuffer = resultKeyBlock->getTuple(leftKeyBlockIdx + rightKeyBlockIdx);
    }
}

// This function returns true if the value in the leftTupleBuffer is larger than the value in the
// rightTupleBuffer.
bool KeyBlockMerger::compareTupleBuffer(uint8_t* leftTupleBuffer, uint8_t* rightTupleBuffer) const {
    if (stringAndUnstructuredKeyColInfo.empty()) {
        // If there is no string or unstructured columns in the keys, we just need to do a
        // simple memcmp.
        return memcmp(leftTupleBuffer, rightTupleBuffer, numBytesPerTuple - sizeof(uint64_t)) > 0;
    } else {
        // We can't simply use memcmp to compare tuples if there are string or unstructured columns.
        // We should only compare the binary strings starting from the last compared string column
        // till the next string column.
        uint64_t lastComparedBytes = 0;
        for (auto& stringAndUnstructuredKeyInfo : stringAndUnstructuredKeyColInfo) {
            auto result =
                memcmp(leftTupleBuffer + lastComparedBytes, rightTupleBuffer + lastComparedBytes,
                    stringAndUnstructuredKeyInfo.colOffsetInEncodedKeyBlock - lastComparedBytes +
                        stringAndUnstructuredKeyInfo.getEncodingSize());
            // If both sides are nulls, we can just continue to check the next string or
            // unstructured column.
            if (OrderByKeyEncoder::isNullVal(
                    leftTupleBuffer, stringAndUnstructuredKeyInfo.isAscOrder) &&
                OrderByKeyEncoder::isNullVal(
                    rightTupleBuffer, stringAndUnstructuredKeyInfo.isAscOrder)) {
                lastComparedBytes = stringAndUnstructuredKeyInfo.colOffsetInEncodedKeyBlock +
                                    stringAndUnstructuredKeyInfo.getEncodingSize();
                continue;
            }
            // If there is a tie, we need to compare the overflow ptr of strings or the actual
            // unstructured values.
            if (result == 0) {
                auto leftEncodedFactorizedTableIdxAndTupleIdx =
                    getEncodedFactorizedTableIdxAndTupleIdx(leftTupleBuffer);
                auto rightEncodedFactorizedTableIdxAnTupleIdx =
                    getEncodedFactorizedTableIdxAndTupleIdx(rightTupleBuffer);
                auto leftFactorizedTable =
                    factorizedTables[leftEncodedFactorizedTableIdxAndTupleIdx.first];
                auto rightFactorizedTable =
                    factorizedTables[rightEncodedFactorizedTableIdxAnTupleIdx.first];
                uint64_t colIdxInFactorizedTable =
                    stringAndUnstructuredKeyInfo.colIdxInFactorizedTable;
                uint8_t result;
                if (stringAndUnstructuredKeyInfo.isStrCol) {
                    Equals::operation<gf_string_t, gf_string_t>(
                        leftFactorizedTable->getString(
                            leftEncodedFactorizedTableIdxAndTupleIdx.second,
                            colIdxInFactorizedTable),
                        rightFactorizedTable->getString(
                            rightEncodedFactorizedTableIdxAnTupleIdx.second,
                            colIdxInFactorizedTable),
                        result, false, false);
                } else {
                    Equals::operation<Value, Value>(
                        leftFactorizedTable->getUnstrValue(
                            leftEncodedFactorizedTableIdxAndTupleIdx.second,
                            colIdxInFactorizedTable),
                        rightFactorizedTable->getUnstrValue(
                            rightEncodedFactorizedTableIdxAnTupleIdx.second,
                            colIdxInFactorizedTable),
                        result, false, false);
                }
                if (result) {
                    // If the tie can't be solved, we need to check the next string or unstructured
                    // column.
                    lastComparedBytes = stringAndUnstructuredKeyInfo.colOffsetInEncodedKeyBlock +
                                        stringAndUnstructuredKeyInfo.getEncodingSize();
                    continue;
                }
                if (stringAndUnstructuredKeyInfo.isStrCol) {
                    GreaterThan::operation<gf_string_t, gf_string_t>(
                        leftFactorizedTable->getString(
                            leftEncodedFactorizedTableIdxAndTupleIdx.second,
                            colIdxInFactorizedTable),
                        rightFactorizedTable->getString(
                            rightEncodedFactorizedTableIdxAnTupleIdx.second,
                            colIdxInFactorizedTable),
                        result, false, false);
                } else {
                    GreaterThan::operation<Value, Value>(
                        leftFactorizedTable->getUnstrValue(
                            leftEncodedFactorizedTableIdxAndTupleIdx.second,
                            colIdxInFactorizedTable),
                        rightFactorizedTable->getUnstrValue(
                            rightEncodedFactorizedTableIdxAnTupleIdx.second,
                            colIdxInFactorizedTable),
                        result, false, false);
                }
                return stringAndUnstructuredKeyInfo.isAscOrder == result;
            }
            return result > 0;
        }
        // The string or unstructured tie can't be solved, just add the tuple in the leftMemBlock to
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
        auto resultKeyBlock = make_shared<MergedKeyBlocks>(leftKeyBlock->getNumBytesPerTuple(),
            leftKeyBlock->getNumTuples() + rightKeyBlock->getNumTuples(), memoryManager);
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

void KeyBlockMergeTaskDispatcher::initIfNecessary(MemoryManager* memoryManager,
    shared_ptr<queue<shared_ptr<MergedKeyBlocks>>> sortedKeyBlocks,
    vector<shared_ptr<FactorizedTable>>& factorizedTables,
    vector<StringAndUnstructuredKeyColInfo>& stringAndUnstructuredKeyColInfo,
    uint64_t numBytesPerTuple) {
    lock_guard<mutex> keyBlockMergeDispatcherLock{mtx};
    if (isInitialized) {
        return;
    }
    isInitialized = true;
    this->memoryManager = memoryManager;
    this->sortedKeyBlocks = sortedKeyBlocks;
    this->keyBlockMerger = make_unique<KeyBlockMerger>(
        factorizedTables, stringAndUnstructuredKeyColInfo, numBytesPerTuple);
}

} // namespace processor
} // namespace graphflow
