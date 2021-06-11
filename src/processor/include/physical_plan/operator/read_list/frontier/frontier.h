#pragma once

#include "src/common/include/memory_manager.h"
#include "src/common/include/types.h"
#include "src/common/include/vector/node_vector.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

constexpr const uint64_t DEFAULT_BLOCK_SIZE = 1 << 18; // 256KB

// Frontier(s) are used by FrontierExtend operator. The Frontier class is the base class for the
// FrontierBag and FrontierSet classes. A FrontierBag is a data structure storing (node_offset,
// multiplicity) pairs where two pairs can have the same node_offset. A FrontierSet is a data
// structure storing a set of node_offsets and a multiplicity associated with each node_offset.
class Frontier {
public:
    Frontier() : moduloSlotBitMask{0u}, currOverflowOffset{0u} {};
    inline void setMemoryManager(MemoryManager* memMan) { this->memMan = memMan; }

public:
    uint64_t moduloSlotBitMask;

protected:
    uint64_t currOverflowOffset;
    vector<unique_ptr<MemoryBlock>> overflowBlocks;
    MemoryManager* memMan;
};

} // namespace processor
} // namespace graphflow
