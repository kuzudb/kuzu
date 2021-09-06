#pragma once

#include "src/processor/include/physical_plan/operator/read_list/frontier/frontier.h"

namespace graphflow {
namespace processor {

// A FrontierBag stores (node_offset, multiplicity) pairs such that any two pairs p1 and p2 can have
// the same node_offset. FrontierBag provides an append(pair) interface. The frontier is a HashTable
// made of a single main Block and possibly a set of overflow blocks. All blocks are obtained from
// the Memory manager. The main block has 32 slots and each slot is a SlotNodeIDs struct value while
// the overflow blocks make up linked lists to handle collisions through chaining once a slot is
// full. Each (node_offset, multiplicity) pair in the chained linked list is a NodeIDOverflow
// struct value.
//
// A FrontierBag is the data structure used to create frontier local to the thread in the
// ExtendFrontier operator. The hash function used in append(pair) is % 32 leading the pair to be
// appended to one of the slot if the slot is not full otherwise follows an overflow pointer. Since
// the data structure expects a single writer, no coordination is required.

const uint64_t NODE_IDS_PER_SLOT_FOR_BAG = 510;

struct NodeIDOverflow {
    node_offset_t nodeOffset;
    uint64_t multiplicity;
    NodeIDOverflow* next;
};

struct SlotNodeIDs {
    uint32_t size;
    node_offset_t nodeOffsets[NODE_IDS_PER_SLOT_FOR_BAG];
    uint64_t multiplicity[NODE_IDS_PER_SLOT_FOR_BAG];
    NodeIDOverflow* next;
    NodeIDOverflow* tail;
};

constexpr const uint64_t NUM_SLOTS_BAG = DEFAULT_BLOCK_SIZE / sizeof(SlotNodeIDs); // 32 = 2^5.

class FrontierBag : public Frontier {

public:
    explicit FrontierBag();

    void initHashTable();
    void append(const ValueVector& vector, uint64_t multiplicity);

private:
    NodeIDOverflow* getOverflowPtr();

public:
    uint64_t size;
    SlotNodeIDs* hashTable;

private:
    unique_ptr<MemoryBlock> mainBlock;
};

} // namespace processor
} // namespace graphflow
