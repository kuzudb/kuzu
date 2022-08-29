#pragma once

#include "src/processor/include/physical_plan/hash_table/base_hash_table.h"

using namespace graphflow::common;
using namespace graphflow::function::operation;

namespace graphflow {
namespace processor {

struct IntersectSlot {
    uint8_t* data;
    uint32_t numValues;
};

// nodeID -> list buffer.
// Sparse slots for nodeIDs. Each slot contains an overflow_value.
class IntersectHashTable : public BaseHashTable {
public:
    IntersectHashTable(MemoryManager& memoryManager, uint64_t numTuples);

    inline overflow_value_t probe(node_offset_t key) {
        auto slot = getSlot(key);
        return overflow_value_t{slot->numValues, slot->data};
    }
    inline IntersectSlot* getSlot(uint64_t slotIdx) {
        auto blockIdx = slotIdx >> numSlotsPerBlockLog2;
        auto idxInBlock = slotIdx - (blockIdx << numSlotsPerBlockLog2);
        return (IntersectSlot*)hashSlotsBlocks[blockIdx]->getData() + idxInBlock;
    }

public:
    mutex mtx;
    vector<unique_ptr<DataBlock>> payloadBlocks;
};

} // namespace processor
} // namespace graphflow
