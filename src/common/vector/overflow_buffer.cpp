#include "src/common/include/vector/overflow_buffer.h"

namespace graphflow {
namespace common {

void OverflowBuffer::allocateLargeStringIfNecessary(gf_string_t& result, uint64_t len) {
    if (gf_string_t::isShortString(len))
        return;
    if (requireNewBlock(len)) {
        allocateNewBlock();
    }
    result.overflowPtr =
        reinterpret_cast<uint64_t>(currentBlock->block->data + currentBlock->currentOffset);
    currentBlock->currentOffset += len;
}

void OverflowBuffer::allocateList(gf_list_t& list) {
    auto sizeToAllocate = TypeUtils::getDataTypeSize(list.childType) * list.capacity;
    if (requireNewBlock(sizeToAllocate)) {
        allocateNewBlock();
    }
    list.overflowPtr =
        reinterpret_cast<uint64_t>(currentBlock->block->data + currentBlock->currentOffset);
    currentBlock->currentOffset += sizeToAllocate;
}

void OverflowBuffer::allocateNewBlock() {
    auto newBlock = make_unique<BufferBlock>(
        memoryManager->allocateBMBackedBlock(false /* do not initialize to zero */));
    currentBlock = newBlock.get();
    blocks.push_back(move(newBlock));
}

} // namespace common
} // namespace graphflow
