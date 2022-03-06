#pragma once

#include <vector>

#include "src/common/include/gf_string.h"
#include "src/common/include/memory_manager.h"
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

class StringBuffer {

public:
    explicit StringBuffer(MemoryManager* memoryManager)
        : memoryManager{memoryManager}, currentBlock{nullptr} {};

    // The blocks StringBuffer uses are allocated through the MemoryManager but are backed by the
    // BufferManager. We need to therefore release them back by calling
    // memoryManager->freeBMBackedBlock.
    ~StringBuffer() {
        for (auto i = 0u; i < blocks.size(); ++i) {
            memoryManager->freeBMBackedBlock(blocks[i]->block->pageIdx);
        }
    }

public:
    void allocateLargeStringIfNecessary(gf_string_t& result, uint64_t len);

public:
    vector<unique_ptr<BufferBlock>> blocks;

    inline void merge(StringBuffer& other) {
        move(begin(other.blocks), end(other.blocks), back_inserter(blocks));
        // We clear the other StringBuffer's block because when it is deconstructed, StringBuffer's
        // deconstructed tries to free these pages by calling memoryManager->freeBMBackedBlock,
        // but it should not because this StringBuffer still needs them.
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
