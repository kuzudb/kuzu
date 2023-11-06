#pragma once

#include "common/utils.h"
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

protected:
    inline void setMaxNumHashSlots(uint64_t newSize) {
        maxNumHashSlots = newSize;
        bitmask = maxNumHashSlots - 1;
    }

    inline void initSlotConstant(uint64_t numSlotsPerBlock_) {
        KU_ASSERT(numSlotsPerBlock_ == common::nextPowerOfTwo(numSlotsPerBlock_));
        numSlotsPerBlock = numSlotsPerBlock_;
        numSlotsPerBlockLog2 = std::log2(numSlotsPerBlock);
        slotIdxInBlockMask =
            common::BitmaskUtils::all1sMaskForLeastSignificantBits(numSlotsPerBlockLog2);
    }

    inline uint64_t getSlotIdxForHash(common::hash_t hash) const { return hash & bitmask; }

protected:
    uint64_t maxNumHashSlots;
    uint64_t bitmask;
    uint64_t numSlotsPerBlock;
    uint64_t numSlotsPerBlockLog2;
    uint64_t slotIdxInBlockMask;
    std::vector<std::unique_ptr<DataBlock>> hashSlotsBlocks;
    storage::MemoryManager& memoryManager;
    std::unique_ptr<FactorizedTable> factorizedTable;
};

} // namespace processor
} // namespace kuzu
