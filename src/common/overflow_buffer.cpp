#include "src/common/include/overflow_buffer.h"

namespace graphflow {
namespace common {

uint8_t* OverflowBuffer::allocateSpace(uint64_t size) {
    assert(size <= LARGE_PAGE_SIZE);
    if (requireNewBlock(size)) {
        allocateNewBlock();
    }
    auto data = currentBlock->block->data + currentBlock->currentOffset;
    currentBlock->currentOffset += size;
    return data;
}

void OverflowBuffer::allocateNewBlock() {
    auto newBlock = make_unique<BufferBlock>(
        memoryManager->allocateBlock(false /* do not initialize to zero */));
    currentBlock = newBlock.get();
    blocks.push_back(move(newBlock));
}

} // namespace common
} // namespace graphflow
