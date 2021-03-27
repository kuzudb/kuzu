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

    // Find free space in existing memory blocks, and copy
    for (auto& block : blocks) {
        if (block->freeSize >= valuesLength) {
            auto blockPtr = block->blockPtr + OVERFLOW_BLOCK_SIZE - block->freeSize;
            for (uint64_t i = 0; i < vector.getNumSelectedValues(); i++) {
                memcpy(blockPtr + (i * numBytesPerValue),
                    vectorValues + (vectorSelectedValuesPos[i] * numBytesPerValue),
                    numBytesPerValue);
            }
            block->freeSize -= valuesLength;
            return overflow_value_t{valuesLength, blockPtr};
        }
    }

    // Allocate a new memory block and copy
    auto blockHandle = memoryManager.allocateBlock(OVERFLOW_BLOCK_SIZE);
    memset(blockHandle->blockPtr, 0, OVERFLOW_BLOCK_SIZE);
    auto blockPtr = blockHandle->blockPtr;
    for (uint64_t i = 0; i < vector.getNumSelectedValues(); i++) {
        memcpy(blockPtr + (i * numBytesPerValue),
            vectorValues + (vectorSelectedValuesPos[i] * numBytesPerValue), numBytesPerValue);
    }
    blockHandle->freeSize -= valuesLength;
    blocks.push_back(move(blockHandle));
    return overflow_value_t{valuesLength, blockPtr};
}

} // namespace processor
} // namespace graphflow
