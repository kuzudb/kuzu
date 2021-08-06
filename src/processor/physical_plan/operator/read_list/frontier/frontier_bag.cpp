#include "src/processor/include/physical_plan/operator/read_list/frontier/frontier_bag.h"

namespace graphflow {
namespace processor {

FrontierBag::FrontierBag() : Frontier(), size{0u} {
    moduloSlotBitMask = (uint32_t)NUM_SLOTS_BAG - 1;
}

void FrontierBag::initHashTable() {
    mainBlock = memMan->allocateBlock(DEFAULT_BLOCK_SIZE);
    hashTable = (SlotNodeIDs*)mainBlock->data;
}

NodeIDOverflow* FrontierBag::getOverflowPtr() {
    auto elementSize = sizeof(NodeIDOverflow);
    if (overflowBlocks.empty() || currOverflowOffset + elementSize >= DEFAULT_BLOCK_SIZE) {
        overflowBlocks.push_back(memMan->allocateBlock(DEFAULT_BLOCK_SIZE));
        currOverflowOffset = 0;
    }
    currOverflowOffset += elementSize;
    auto pos = overflowBlocks.size() - 1;
    return (NodeIDOverflow*)(overflowBlocks[pos]->data + currOverflowOffset);
}

void FrontierBag::append(const NodeIDVector& vector, uint64_t multiplicity) {
    for (auto i = 0u; i < vector.state->selectedSize; i++) {
        auto nodeOffset = vector.readNodeOffset(i);
        auto slot = nodeOffset & moduloSlotBitMask;
        if (hashTable[slot].size < NODE_IDS_PER_SLOT_FOR_BAG) {
            hashTable[slot].nodeOffsets[hashTable[slot].size] = nodeOffset;
            hashTable[slot].multiplicity[hashTable[slot].size++] = multiplicity;
        } else {
            auto overflowPtr = getOverflowPtr();
            overflowPtr->nodeOffset = nodeOffset;
            overflowPtr->multiplicity = multiplicity;
            if (hashTable[slot].tail == nullptr) {
                hashTable[slot].next = hashTable[slot].tail = overflowPtr;
            } else {
                hashTable[slot].tail->next = overflowPtr;
                hashTable[slot].tail = overflowPtr;
            }
            hashTable[slot].size++;
        }
    }
    size += vector.state->selectedSize;
}

} // namespace processor
} // namespace graphflow
