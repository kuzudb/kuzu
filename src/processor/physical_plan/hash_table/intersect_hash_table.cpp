#include "src/processor/include/physical_plan/hash_table/intersect_hash_table.h"

namespace graphflow {
namespace processor {

IntersectHashTable::IntersectHashTable(MemoryManager& memoryManager, uint64_t numNodes)
    : BaseHashTable{memoryManager} {
    auto numSlotsPerBlock = LARGE_PAGE_SIZE / sizeof(IntersectSlot);
    numSlotsPerBlockLog2 = log2(numSlotsPerBlock);
    auto numBlocksForSlots = (numNodes + numSlotsPerBlock - 1) / numSlotsPerBlock;
    for (auto i = 0u; i < numBlocksForSlots; i++) {
        hashSlotsBlocks.emplace_back(make_unique<DataBlock>(&memoryManager));
    }
}

} // namespace processor
} // namespace graphflow
