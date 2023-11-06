#include "common/in_mem_overflow_buffer.h"

namespace kuzu {
namespace common {

uint8_t* InMemOverflowBuffer::allocateSpace(uint64_t size) {
    KU_ASSERT(size <= BufferPoolConstants::PAGE_256KB_SIZE);
    if (requireNewBlock(size)) {
        allocateNewBlock();
    }
    auto data = currentBlock->block->buffer + currentBlock->currentOffset;
    currentBlock->currentOffset += size;
    return data;
}

void InMemOverflowBuffer::allocateNewBlock() {
    auto newBlock = make_unique<BufferBlock>(
        memoryManager->allocateBuffer(false /* do not initialize to zero */));
    currentBlock = newBlock.get();
    blocks.push_back(std::move(newBlock));
}

} // namespace common
} // namespace kuzu
