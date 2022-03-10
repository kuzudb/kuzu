#pragma once

#include <vector>

#include "src/common/include/memory_manager.h"
#include "src/common/types/include/type_utils.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/file_handle.h"

namespace graphflow {
namespace common {

using namespace graphflow::storage;

struct BufferBlock {
public:
    explicit BufferBlock(unique_ptr<BMBackedMemoryBlock> block)
        : size{block->size}, currentOffset{0}, block{move(block)} {}

public:
    uint64_t size;
    uint64_t currentOffset;
    unique_ptr<BMBackedMemoryBlock> block;

    inline void resetCurrentOffset() { currentOffset = 0; }
};

class OverflowBuffer {

public:
    explicit OverflowBuffer(MemoryManager* memoryManager)
        : memoryManager{memoryManager}, currentBlock{nullptr} {};

    // The blocks used are allocated through the MemoryManager but are backed by the
    // BufferManager. We need to therefore release them back by calling
    // memoryManager->freeBMBackedBlock.
    ~OverflowBuffer() {
        for (auto i = 0u; i < blocks.size(); ++i) {
            memoryManager->freeBMBackedBlock(blocks[i]->block->pageIdx);
        }
    }

    void allocateLargeStringIfNecessary(gf_string_t& result, uint64_t len);

    void allocateList(gf_list_t& list);

private:
    inline bool requireNewBlock(uint64_t sizeToAllocate) {
        if (sizeToAllocate > LARGE_PAGE_SIZE) {
            throw invalid_argument("Require size " + to_string(sizeToAllocate) +
                                   " greater than single block size " + to_string(LARGE_PAGE_SIZE) +
                                   ".");
        }
        return currentBlock == nullptr ||
               (currentBlock->currentOffset + sizeToAllocate) > currentBlock->size;
    }

    // TODO: more than more one block might be needed?
    void allocateNewBlock();

public:
    vector<unique_ptr<BufferBlock>> blocks;

    inline void merge(OverflowBuffer& other) {
        move(begin(other.blocks), end(other.blocks), back_inserter(blocks));
        // We clear the other OverflowBuffer's block because when it is deconstructed,
        // OverflowBuffer's deconstructed tries to free these pages by calling
        // memoryManager->freeBMBackedBlock, but it should not because this OverflowBuffer still
        // needs them.
        other.blocks.clear();
        currentBlock = other.currentBlock;
    }

    // Releases all memory accumulated for string overflows so far and reinitializes its state to an
    // empty buffer. If there a large string that used point to any of these overflow buffers they
    // will error.
    inline void resetBuffer() {
        if (blocks.size() >= 1) {
            auto firstBlock = move(blocks[0]);
            for (auto i = 1u; i < blocks.size(); ++i) {
                memoryManager->freeBMBackedBlock(blocks[i]->block->pageIdx);
            }
            blocks.clear();
            firstBlock->resetCurrentOffset();
            blocks.push_back(move(firstBlock));
        }
        if (!blocks.empty()) {
            currentBlock = blocks[0].get();
        }
    }

private:
    MemoryManager* memoryManager;
    BufferBlock* currentBlock;
};

} // namespace common
} // namespace graphflow
