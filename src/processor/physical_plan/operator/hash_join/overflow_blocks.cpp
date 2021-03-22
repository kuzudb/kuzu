#include "src/processor/include/physical_plan/operator/hash_join/overflow_blocks.h"

using namespace std;

namespace graphflow {
namespace processor {

overflow_value_t OverflowBlocks::addValue(uint8_t* valuePtr, uint64_t valueLength) {
    if (valueLength > OVERFLOW_BLOCK_SIZE) {
        throw invalid_argument("Value length is larger than a overflow block.");
    }
    for (auto& block : blocks) {
        if (block->freeSize >= valueLength) {
            auto pos = block->blockPtr + OVERFLOW_BLOCK_SIZE - block->freeSize;
            memcpy(pos, valuePtr, valueLength);
            block->freeSize -= valueLength;
            return overflow_value_t{valueLength, pos};
        }
    }
    auto listBlockHandle = memoryManager.allocateBlock(OVERFLOW_BLOCK_SIZE);
    memset(listBlockHandle->blockPtr, 0, OVERFLOW_BLOCK_SIZE);
    auto listPtr = listBlockHandle->blockPtr;
    memcpy(listPtr, valuePtr, valueLength);
    listBlockHandle->freeSize -= valueLength;
    blocks.push_back(move(listBlockHandle));
    return overflow_value_t{valueLength, listPtr};
}

} // namespace processor
} // namespace graphflow
