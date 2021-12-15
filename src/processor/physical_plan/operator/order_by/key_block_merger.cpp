#include "src/processor/include/physical_plan/operator/order_by/key_block_merger.h"

#include "src/common/include/operations/comparison_operations.h"

using namespace graphflow::processor;
using namespace graphflow::common::operation;

namespace graphflow {
namespace processor {

void KeyBlockMerger::mergeKeyBlocks(KeyBlockMergeInfo& leftKeyBlockInfo,
    KeyBlockMergeInfo& rightKeyBlockInfo, KeyBlockMergeInfo& resultKeyBlockInfo) {
    auto leftKeyBlockIdx = leftKeyBlockInfo.startRowIdx;
    auto rightKeyBlockIdx = rightKeyBlockInfo.startRowIdx;
    auto resultKeyBlockIdx = resultKeyBlockInfo.startRowIdx;
    auto leftKeyBlockBuffer = leftKeyBlockInfo.memBlock.data +
                              leftKeyBlockIdx * orderByKeyEncoder.getKeyBlockEntrySizeInBytes();
    auto rightKeyBlockBuffer = rightKeyBlockInfo.memBlock.data +
                               rightKeyBlockIdx * orderByKeyEncoder.getKeyBlockEntrySizeInBytes();
    auto resultKeyBlockBuffer = resultKeyBlockInfo.memBlock.data +
                                resultKeyBlockIdx * orderByKeyEncoder.getKeyBlockEntrySizeInBytes();

    while (leftKeyBlockIdx < leftKeyBlockInfo.endRowIdx &&
           rightKeyBlockIdx < rightKeyBlockInfo.endRowIdx) {
        if (compareRowBuffer(leftKeyBlockBuffer, rightKeyBlockBuffer)) {
            memcpy(resultKeyBlockBuffer, rightKeyBlockBuffer,
                orderByKeyEncoder.getKeyBlockEntrySizeInBytes());
            rightKeyBlockBuffer += orderByKeyEncoder.getKeyBlockEntrySizeInBytes();
            rightKeyBlockIdx++;
        } else {
            memcpy(resultKeyBlockBuffer, leftKeyBlockBuffer,
                orderByKeyEncoder.getKeyBlockEntrySizeInBytes());
            leftKeyBlockBuffer += orderByKeyEncoder.getKeyBlockEntrySizeInBytes();
            leftKeyBlockIdx++;
        }
        resultKeyBlockBuffer += orderByKeyEncoder.getKeyBlockEntrySizeInBytes();
    }

    // If there are still unmerged rows in the left or right memBlock, just append them to the
    // result memBlock.
    if (leftKeyBlockIdx < leftKeyBlockInfo.endRowIdx) {
        memcpy(resultKeyBlockBuffer, leftKeyBlockBuffer,
            orderByKeyEncoder.getKeyBlockEntrySizeInBytes() *
                (leftKeyBlockInfo.endRowIdx - leftKeyBlockIdx));
    } else if (rightKeyBlockIdx < rightKeyBlockInfo.endRowIdx) {
        memcpy(resultKeyBlockBuffer, rightKeyBlockBuffer,
            orderByKeyEncoder.getKeyBlockEntrySizeInBytes() *
                (rightKeyBlockInfo.endRowIdx - rightKeyBlockIdx));
    }
}

// This function returns true if the value in the leftRowBuffer is larger than the value in the
// rightRowBuffer.
bool KeyBlockMerger::compareRowBuffer(uint8_t* leftRowBuffer, uint8_t* rightRowBuffer) {
    if (strKeyColInfo.empty()) {
        // If there is no string columns in the keys, we just need to do a simple memcmp.
        return memcmp(leftRowBuffer, rightRowBuffer,
                   orderByKeyEncoder.getKeyBlockEntrySizeInBytes() - sizeof(uint64_t)) > 0;
    } else {
        // we can't simply use memcmp to compare rows if there are string columns. we should only
        // compare the binary strings starting from the last compared string column till the next
        // string column.
        uint64_t lastComparedBytes = 0;
        for (auto& strKeyCol : strKeyColInfo) {
            auto result =
                memcmp(leftRowBuffer + lastComparedBytes, rightRowBuffer + lastComparedBytes,
                    strKeyCol.colOffsetInEncodedKeyBlock - lastComparedBytes +
                        orderByKeyEncoder.getEncodingSize(STRING));

            // If there is a tie, we need to compare the overflow ptr of strings.
            if (result == 0) {
                auto leftStr = getStrFromRowCollection(leftRowBuffer, strKeyCol);
                auto rightStr = getStrFromRowCollection(rightRowBuffer, strKeyCol);
                uint8_t result;
                Equals::operation<gf_string_t, gf_string_t>(
                    leftStr, rightStr, result, false, false);
                if (result) {
                    // If the tie can't be solved, we need to check the next string column.
                    lastComparedBytes = strKeyCol.colOffsetInEncodedKeyBlock +
                                        orderByKeyEncoder.getEncodingSize(STRING);
                    continue;
                }
                GreaterThan::operation<gf_string_t, gf_string_t>(
                    leftStr, rightStr, result, false, false);
                return strKeyCol.isAscOrder == result;
            }
            return result > 0;
        }
        // The string tie can't be solved, just add the row in the leftMemBlock to resultMemBlock.
        return false;
    }
}

} // namespace processor
} // namespace graphflow
