#include "src/processor/include/physical_plan/operator/order_by/key_block_merge.h"

using namespace graphflow::processor;

unique_ptr<MemoryBlock> KeyBlockMerger::mergeKeyBlocks(
    vector<unique_ptr<MemoryBlock>>& memoryBlocks, vector<KeyBlockMergeIdx>& keyBlockMergeIdx) {
    auto mergedKeyBlock =
        memoryManager.allocateBlock(memoryBlocks[0]->size + memoryBlocks[1]->size);
    auto mergedKeyBlockPtr = mergedKeyBlock->data;
    auto leftKeyBlockIdx = keyBlockMergeIdx[0].first;
    auto rightKeyBlockIdx = keyBlockMergeIdx[1].first;
    auto leftKeyBlockPtr = memoryBlocks[0]->data + leftKeyBlockIdx * entrySizeInBytes;
    auto rightKeyBlockPtr = memoryBlocks[1]->data + rightKeyBlockIdx * entrySizeInBytes;
    while (leftKeyBlockIdx <= keyBlockMergeIdx[0].second &&
           rightKeyBlockIdx <= keyBlockMergeIdx[1].second) {
        if (compareRowPtr(leftKeyBlockPtr, rightKeyBlockPtr, leftKeyBlockIdx, rightKeyBlockIdx)) {
            memcpy(mergedKeyBlockPtr, rightKeyBlockPtr, entrySizeInBytes);
            rightKeyBlockPtr += entrySizeInBytes;
            rightKeyBlockIdx++;
        } else {
            memcpy(mergedKeyBlockPtr, leftKeyBlockPtr, entrySizeInBytes);
            leftKeyBlockPtr += entrySizeInBytes;
            leftKeyBlockIdx++;
        }
        mergedKeyBlockPtr += entrySizeInBytes;
    }
    if (leftKeyBlockIdx <= keyBlockMergeIdx[0].second) {
        memcpy(mergedKeyBlockPtr, leftKeyBlockPtr,
            entrySizeInBytes * (keyBlockMergeIdx[0].second - leftKeyBlockIdx + 1));
    } else if (rightKeyBlockIdx <= keyBlockMergeIdx[1].second) {
        memcpy(mergedKeyBlockPtr, rightKeyBlockPtr,
            entrySizeInBytes * (keyBlockMergeIdx[1].second - rightKeyBlockIdx + 1));
    }
    return mergedKeyBlock;
}

bool KeyBlockMerger::compareRowPtr(uint8_t* leftRowPtr, uint8_t* rightRowPtr,
    uint64_t leftKeyBlockIdx, uint64_t rightKeyBlockIdx) {
    if (strColPos.empty()) {
        // there is no string in order by columns, simply call memcmp to compare binary strings
        return memcmp(leftRowPtr, rightRowPtr, entrySizeInBytes - sizeof(uint64_t)) > 0;
    } else {
        auto comparedBytes = 0u;
        // we can't simply use memcmp to compare rows if there are string columns. we should only
        // compare the binary strings starting from the last compared string column till the next
        // string column.
        for (auto strCol : strColPos) {
            auto result =
                memcmp(leftRowPtr + comparedBytes, rightRowPtr + comparedBytes, strCol.second);
            // if there is a tie, we need to compare the overflow ptr of strings
            if (result == 0) {
                auto strValues = (gf_string_t*)orderByVectors[strCol.first]->values;
                auto leftRowID = *((uint64_t*)(leftRowPtr + entrySizeInBytes - sizeof(uint64_t)));
                auto rightRowID = *((uint64_t*)(rightRowPtr + entrySizeInBytes - sizeof(uint64_t)));
                auto leftStrVal = orderByVectors[strCol.first]->isNull(leftRowID) ?
                                      "" :
                                      strValues[leftRowID].getAsString();
                auto rightStrVal = orderByVectors[strCol.first]->isNull(rightRowID) ?
                                       "" :
                                       strValues[rightRowID].getAsString();
                if (leftStrVal == rightStrVal) {
                    comparedBytes += strCol.second;
                    continue;
                }
                return isAscOrder[strCol.first] ? leftStrVal > rightStrVal :
                                                  rightStrVal > leftStrVal;
            }
            return result > 0;
        }
        return true;
    }
}
