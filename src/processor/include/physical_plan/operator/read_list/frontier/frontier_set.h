#pragma once

#include "src/processor/include/physical_plan/operator/read_list/frontier/frontier_bag.h"

namespace graphflow {
namespace processor {

// A FrontierSet stores (node_offset, multiplicity) pairs such that each pair has a unique
// node_offset value. The FrontierSet is made by a number of main blocks (each block has 8096 slots
// and each slot is a SlotNodeIDAndMultiplicity struct value). The number of slots chosen is
// provided at initialization. The FrontierSet contains further overflow blocks which are used to
// handle collisions through chaining. The hash function is modulo % numSlots where numSlots is
// bigger than that from the frontierBag the input pairs are obtained from to ensure that pairs in a
// slot in a FrontierBag map to unique slots in a global FrontierSet.

// Since the data structure expects multiple writers, coordination is required when requiring
// overflow space.

struct SlotNodeIDAndMultiplicity {
    bool hasValue;
    node_offset_t nodeOffset;
    uint64_t multiplicity;
    SlotNodeIDAndMultiplicity* next;
};

// NUM_SLOTS_PER_BLOCK_SET is power of 2 smaller than:
//     DEFAULT_BLOCK_SIZE / sizeof(SlotNodeIDAndMultiplicity)
constexpr const uint64_t NUM_SLOTS_PER_BLOCK_SET = 8192;

class FrontierSet : public Frontier {
public:
    FrontierSet();

    void initHashTable(uint64_t numSlots);

    void insert(const ValueVector& vector);
    void insert(const FrontierBag& frontier, uint64_t slot);
    void insert(node_offset_t nodeOffset, uint64_t multiplicity);

private:
    SlotNodeIDAndMultiplicity* getOverflowPtr();

public:
    vector<SlotNodeIDAndMultiplicity*> hashTableBlocks;
    uint64_t moduloMainBlockBitMask;

private:
    vector<unique_ptr<MemoryBlock>> mainBlocks;
    mutex overflowLock;
    vector<unique_ptr<MemoryBlock>> overflowBlocks;
};

} // namespace processor
} // namespace graphflow
