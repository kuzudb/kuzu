#include "src/common/include/vector/string_buffer.h"

namespace graphflow {
namespace common {

void StringBuffer::allocateLargeStringIfNecessary(gf_string_t& result, uint64_t len) {
    if (gf_string_t::isShortString(len))
        return;
    if (currentBlock == nullptr || (currentBlock->currentOffset + len) > currentBlock->size) {
        auto newBlock = make_unique<BufferBlock>(
            memoryManager->allocateBMBackedBlock(false /* do not initialize to zero */));
        currentBlock = newBlock.get();
        blocks.push_back(move(newBlock));
    }
    result.overflowPtr =
        reinterpret_cast<uint64_t>(currentBlock->block->data + currentBlock->currentOffset);
    currentBlock->currentOffset += len;
}

} // namespace common
} // namespace graphflow
