#include "src/common/include/vector/string_buffer.h"

#include <cassert>

namespace graphflow {
namespace common {

gf_string_t StringBuffer::allocateLargeString(uint64_t len) {
    assert(len > gf_string_t::SHORT_STR_LENGTH);
    if (currentBlock == nullptr || (currentBlock->currentOffset + len) > currentBlock->size) {
        auto blockSize = max(len, MIN_BUFFER_BLOCK_SIZE);
        auto newBlock = make_unique<BufferBlock>(
            memoryManager.allocateBlock(blockSize, true /* initializeToZero */));
        currentBlock = newBlock.get();
        blocks.push_back(move(newBlock));
    }
    gf_string_t result;
    result.len = len;
    result.overflowPtr =
        reinterpret_cast<uint64_t>(currentBlock->data + currentBlock->currentOffset);
    currentBlock->currentOffset += len;
    return result;
}

void StringBuffer::appendBuffer(StringBuffer& other) {
    if (other.blocks.empty()) {
        return;
    }
    blocks.insert(blocks.end(), make_move_iterator(other.blocks.begin()),
        make_move_iterator(other.blocks.end()));
    currentBlock = other.currentBlock;
}
} // namespace common
} // namespace graphflow
