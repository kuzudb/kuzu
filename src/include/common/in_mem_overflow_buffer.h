#pragma once

#include <iterator>
#include <vector>

#include "common/constants.h"
#include "common/exception/runtime.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace common {

struct BufferBlock {
public:
    explicit BufferBlock(std::unique_ptr<storage::MemoryBuffer> block)
        : size{block->allocator->getPageSize()}, currentOffset{0}, block{std::move(block)} {}

public:
    uint64_t size;
    uint64_t currentOffset;
    std::unique_ptr<storage::MemoryBuffer> block;

    inline void resetCurrentOffset() { currentOffset = 0; }
};

class InMemOverflowBuffer {

public:
    explicit InMemOverflowBuffer(storage::MemoryManager* memoryManager)
        : memoryManager{memoryManager}, currentBlock{nullptr} {};

    uint8_t* allocateSpace(uint64_t size);

    inline void merge(InMemOverflowBuffer& other) {
        move(begin(other.blocks), end(other.blocks), back_inserter(blocks));
        // We clear the other InMemOverflowBuffer's block because when it is deconstructed,
        // InMemOverflowBuffer's deconstructed tries to free these pages by calling
        // memoryManager->freeBlock, but it should not because this InMemOverflowBuffer still
        // needs them.
        other.blocks.clear();
        currentBlock = other.currentBlock;
    }

    // Releases all memory accumulated for string overflows so far and re-initializes its state to
    // an empty buffer. If there is a large string that used point to any of these overflow buffers
    // they will error.
    inline void resetBuffer() {
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

private:
    inline bool requireNewBlock(uint64_t sizeToAllocate) {
        if (sizeToAllocate > BufferPoolConstants::PAGE_256KB_SIZE) {
            throw RuntimeException("Require size " + std::to_string(sizeToAllocate) +
                                   " greater than single block size " +
                                   std::to_string(BufferPoolConstants::PAGE_256KB_SIZE) + ".");
        }
        return currentBlock == nullptr ||
               (currentBlock->currentOffset + sizeToAllocate) > currentBlock->size;
    }

    void allocateNewBlock();

private:
    std::vector<std::unique_ptr<BufferBlock>> blocks;
    storage::MemoryManager* memoryManager;
    BufferBlock* currentBlock;
};

} // namespace common
} // namespace kuzu
