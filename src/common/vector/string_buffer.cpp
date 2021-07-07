#include "src/common/include/vector/string_buffer.h"

#include <cassert>

namespace graphflow {
namespace common {

void StringBuffer::allocateLargeString(gf_string_t& result, uint64_t len) {
    assert(len > gf_string_t::SHORT_STR_LENGTH);
    if (currentBlock == nullptr || (currentBlock->currentOffset + len) > currentBlock->size) {
        auto blockSize = max(len, MIN_BUFFER_BLOCK_SIZE);
        auto newBlock = make_unique<BufferBlock>(
            memoryManager.allocateBlock(blockSize, true /* initializeToZero */));
        currentBlock = newBlock.get();
        blocks.push_back(move(newBlock));
    }
    result.overflowPtr =
        reinterpret_cast<uint64_t>(currentBlock->data + currentBlock->currentOffset);
    currentBlock->currentOffset += len;
}

} // namespace common
} // namespace graphflow
