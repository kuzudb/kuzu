#include "common/in_mem_overflow_buffer.h"

#include "storage/buffer_manager/memory_manager.h"

using namespace kuzu::storage;

namespace kuzu {
namespace common {

BufferBlock::BufferBlock(std::unique_ptr<storage::MemoryBuffer> block)
    : currentOffset{0}, block{std::move(block)} {}

BufferBlock::~BufferBlock() = default;

uint64_t BufferBlock::size() const {
    return block->getBuffer().size();
}

uint8_t* BufferBlock::data() const {
    return block->getBuffer().data();
}

uint8_t* InMemOverflowBuffer::allocateSpace(uint64_t size) {
    if (requireNewBlock(size)) {
        allocateNewBlock(size);
    }
    auto data = currentBlock->data() + currentBlock->currentOffset;
    currentBlock->currentOffset += size;
    return data;
}

void InMemOverflowBuffer::resetBuffer() {
    if (!blocks.empty()) {
        auto firstBlock = std::move(blocks[0]);
        blocks.clear();
        firstBlock->resetCurrentOffset();
        blocks.push_back(std::move(firstBlock));
    }
    if (!blocks.empty()) {
        currentBlock = blocks[0].get();
    }
}

void InMemOverflowBuffer::allocateNewBlock(uint64_t size) {
    auto newBlock = make_unique<BufferBlock>(
        memoryManager->allocateBuffer(false /* do not initialize to zero */, size));
    currentBlock = newBlock.get();
    blocks.push_back(std::move(newBlock));
}

} // namespace common
} // namespace kuzu
