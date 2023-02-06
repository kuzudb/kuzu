#include "common/in_mem_overflow_buffer.h"

namespace kuzu {
namespace common {

uint8_t* InMemOverflowBuffer::allocateSpace(uint64_t size) {
    assert(size <= LARGE_PAGE_SIZE);
    if (requireNewBlock(size)) {
        allocateNewBlock();
    }
    auto data = currentBlock->block->data + currentBlock->currentOffset;
    currentBlock->currentOffset += size;
    return data;
}

void InMemOverflowBuffer::allocateNewBlock() {
    auto newBlock = make_unique<BufferBlock>(
        memoryManager->allocateBlock(false /* do not initialize to zero */));
    currentBlock = newBlock.get();
    blocks.push_back(std::move(newBlock));
}

} // namespace common
} // namespace kuzu
