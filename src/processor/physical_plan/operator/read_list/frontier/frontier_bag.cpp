#include "src/processor/include/physical_plan/operator/read_list/frontier/frontier_bag.h"

namespace graphflow {
namespace processor {

FrontierBag::FrontierBag() : Frontier(), size{0u} {
    moduloSlotBitMask = (uint32_t)NUM_SLOTS_BAG - 1;
}

void FrontierBag::initHashTable() {
    mainBlock = memMan->allocateBlock(DEFAULT_BLOCK_SIZE);
    hashTable = (SlotNodeIDs*)mainBlock->blockPtr;
}

NodeIDOverflow* FrontierBag::getOverflowPtr() {
    auto elementSize = sizeof(NodeIDOverflow);
    if (overflowBlocks.empty() || currOverflowOffset + elementSize >= DEFAULT_BLOCK_SIZE) {
        overflowBlocks.push_back(memMan->allocateBlock(DEFAULT_BLOCK_SIZE));
        currOverflowOffset = 0;
    }
    currOverflowOffset += elementSize;
    auto pos = overflowBlocks.size() - 1;
    return (NodeIDOverflow*)(overflowBlocks[pos]->blockPtr + currOverflowOffset);
}

void FrontierBag::append(const NodeIDVector& vector, uint64_t multiplicity) {
    nodeID_t nodeID;
    for (auto i = 0u; i < vector.state->size; i++) {
        vector.readNodeOffset(i, nodeID);
        auto slot = nodeID.offset & moduloSlotBitMask;
        if (hashTable[slot].size < NODE_IDS_PER_SLOT_FOR_BAG) {
            hashTable[slot].nodeOffsets[hashTable[slot].size] = nodeID.offset;
            hashTable[slot].multiplicity[hashTable[slot].size++] = multiplicity;
        } else {
            auto overflowPtr = getOverflowPtr();
            overflowPtr->nodeOffset = nodeID.offset;
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
    size += vector.state->size;
}

} // namespace processor
} // namespace graphflow
