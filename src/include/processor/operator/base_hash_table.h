#pragma once

#include "common/utils.h"
#include "function/hash/hash_functions.h"
#include "processor/result/factorized_table.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace processor {

class BaseHashTable {
public:
    explicit BaseHashTable(storage::MemoryManager& memoryManager)
        : maxNumHashSlots{0}, bitmask{0}, numSlotsPerBlockLog2{0}, slotIdxInBlockMask{0},
          memoryManager{memoryManager} {}

    virtual ~BaseHashTable() = default;

    inline uint64_t getSlotIdxForHash(common::hash_t hash) const { return hash & bitmask; }

protected:
    uint64_t maxNumHashSlots;
    uint64_t bitmask;
    std::vector<std::unique_ptr<DataBlock>> hashSlotsBlocks;
    uint64_t numSlotsPerBlockLog2;
    uint64_t slotIdxInBlockMask;
    storage::MemoryManager& memoryManager;
    std::unique_ptr<FactorizedTable> factorizedTable;
};

} // namespace processor
} // namespace kuzu
