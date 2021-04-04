#include "src/processor/include/physical_plan/operator/hash_join/overflow_blocks.h"

using namespace std;

namespace graphflow {
namespace processor {

overflow_value_t OverflowBlocks::addVector(ValueVector& vector) {
    auto numBytesPerValue = vector.getNumBytesPerValue();
    auto valuesLength = vector.getNumBytesPerValue() * vector.getNumSelectedValues();
    auto vectorValues = vector.getValues();
    auto vectorSelectedValuesPos = vector.getSelectedValuesPos();
    if (valuesLength > OVERFLOW_BLOCK_SIZE) {
        throw invalid_argument("Value length is larger than a overflow block.");
    }

    uint8_t* blockAppendPos = nullptr;
    {
        lock_guard appendLock(overflowBlocksLock);
        // Find free space in existing memory blocks
        for (auto& blockHandle : blocks) {
            if (blockHandle->freeSize >= valuesLength) {
                blockAppendPos =
                    blockHandle->blockPtr + OVERFLOW_BLOCK_SIZE - blockHandle->freeSize;
                blockHandle->freeSize -= valuesLength;
            }
        }
        if (blockAppendPos == nullptr) {
            // If no free space found in existing memory blocks, allocate a new one
            auto blockHandle = memManager->allocateBlock(OVERFLOW_BLOCK_SIZE);
            memset(blockHandle->blockPtr, 0, OVERFLOW_BLOCK_SIZE);
            blockAppendPos = blockHandle->blockPtr;
            blockHandle->freeSize -= valuesLength;
            blocks.push_back(move(blockHandle));
        }
    }

    for (uint64_t i = 0; i < vector.getNumSelectedValues(); i++) {
        memcpy(blockAppendPos + (i * numBytesPerValue),
            vectorValues + (vectorSelectedValuesPos[i] * numBytesPerValue), numBytesPerValue);
    }
    return overflow_value_t{valuesLength, blockAppendPos};
}

} // namespace processor
} // namespace graphflow
