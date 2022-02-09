#include "src/processor/include/physical_plan/operator/read_list/frontier/frontier_set.h"

namespace graphflow {
namespace processor {

FrontierSet::FrontierSet() : Frontier() {
    currOverflowOffset = 0u;
}

void FrontierSet::initHashTable(uint64_t numSlots) {
    auto numBlocks = numSlots < NUM_SLOTS_PER_BLOCK_SET ? 1 : numSlots / NUM_SLOTS_PER_BLOCK_SET;
    for (auto i = 0u; i < numBlocks; i++) {
        auto mainBlock = memMan->allocateBlock(DEFAULT_BLOCK_SIZE);
        hashTableBlocks.push_back((SlotNodeIDAndMultiplicity*)mainBlock->data);
        mainBlocks.push_back(move(mainBlock));
    }
    moduloMainBlockBitMask = numBlocks - 1;
    moduloSlotBitMask = numBlocks == 1 ? numSlots - 1 : NUM_SLOTS_PER_BLOCK_SET - 1;
}

SlotNodeIDAndMultiplicity* FrontierSet::getOverflowPtr() {
    SlotNodeIDAndMultiplicity* ptr;
    {
        // We lock to update the currOverflowOffset as we have multiple threads updating the
        // frontierSet all of which request an overflow ptr in the same overflow block.
        lock_guard<mutex> lock(overflowLock);
        auto elementSize = sizeof(SlotNodeIDAndMultiplicity);
        if (overflowBlocks.empty() || currOverflowOffset + elementSize >= DEFAULT_BLOCK_SIZE) {
            overflowBlocks.push_back(memMan->allocateBlock(DEFAULT_BLOCK_SIZE));
            currOverflowOffset = 0;
        } else {
            currOverflowOffset += elementSize;
        }
        auto pos = overflowBlocks.size() - 1;
        ptr = (SlotNodeIDAndMultiplicity*)(overflowBlocks[pos]->data + currOverflowOffset);
    }
    return ptr;
}

void FrontierSet::insert(const FrontierBag& frontier, uint64_t slot) {
    NodeIDOverflow* value = nullptr;
    for (auto i = 0u; i < frontier.hashTable[slot].size; i++) {
        node_offset_t nodeOffset;
        uint64_t multiplicity;
        if (i < NODE_IDS_PER_SLOT_FOR_BAG) {
            nodeOffset = frontier.hashTable[slot].nodeOffsets[i];
            multiplicity = frontier.hashTable[slot].multiplicity[i];
        } else if (i == NODE_IDS_PER_SLOT_FOR_BAG) {
            value = frontier.hashTable[slot].next;
            nodeOffset = value->nodeOffset;
            multiplicity = value->multiplicity;
        } else {
            value = value->next;
            nodeOffset = value->nodeOffset;
            multiplicity = value->multiplicity;
        }
        insert(nodeOffset, multiplicity);
    }
}

void FrontierSet::insert(node_offset_t nodeOffset, uint64_t multiplicity) {
    auto htBlock = hashTableBlocks[(nodeOffset >> 12u) & moduloMainBlockBitMask];
    auto slot = nodeOffset & moduloSlotBitMask;
    if (htBlock[slot].hasValue) {
        if (htBlock[slot].nodeOffset == nodeOffset) {
            htBlock[slot].multiplicity += multiplicity;
        } else {
            auto found = false;
            auto value = &htBlock[slot];
            while (value->next) {
                if (value->next->nodeOffset == nodeOffset) {
                    htBlock[slot].multiplicity += multiplicity;
                    found = true;
                    break;
                }
                value = value->next;
            }
            if (!found) {
                auto overflowPtr = getOverflowPtr();
                overflowPtr->nodeOffset = nodeOffset;
                overflowPtr->multiplicity = multiplicity;
                value->next = overflowPtr;
            }
        }
    } else {
        htBlock[slot].hasValue = true;
        htBlock[slot].nodeOffset = nodeOffset;
        htBlock[slot].multiplicity = multiplicity;
    }
}

void FrontierSet::insert(const ValueVector& vector) {
    for (auto pos = 0u; pos < vector.state->selectedSize; pos++) {
        insert(vector.readNodeOffset(pos), 1 /* multiplicity */);
    }
}

} // namespace processor
} // namespace graphflow
