#pragma once

#include "common/types/types.h"
#include "processor/result/factorized_table.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace processor {

using compare_function_t = std::function<bool(common::ValueVector*, uint32_t, const uint8_t*)>;

class BaseHashTable {
public:
    BaseHashTable(storage::MemoryManager& memoryManager, common::logical_type_vec_t keyTypes);

    virtual ~BaseHashTable() = default;

protected:
    static constexpr uint64_t HASH_BLOCK_SIZE = common::TEMP_PAGE_SIZE;

    uint64_t getSlotIdxForHash(common::hash_t hash) const { return hash & bitmask; }
    void setMaxNumHashSlots(uint64_t newSize);
    void computeAndCombineVecHash(const std::vector<common::ValueVector*>& unFlatKeyVectors,
        uint32_t startVecIdx);
    void computeVectorHashes(const std::vector<common::ValueVector*>& flatKeyVectors,
        const std::vector<common::ValueVector*>& unFlatKeyVectors);
    void initSlotConstant(uint64_t numSlotsPerBlock);
    bool matchFlatVecWithEntry(const std::vector<common::ValueVector*>& keyVectors,
        const uint8_t* entry);

private:
    void initCompareFuncs();
    void initTmpHashVector();

protected:
    uint64_t maxNumHashSlots;
    uint64_t bitmask;
    uint64_t numSlotsPerBlockLog2;
    uint64_t slotIdxInBlockMask;
    std::vector<std::unique_ptr<DataBlock>> hashSlotsBlocks;
    storage::MemoryManager& memoryManager;
    std::unique_ptr<FactorizedTable> factorizedTable;
    std::vector<compare_function_t> compareEntryFuncs;
    std::vector<common::LogicalType> keyTypes;
    // Temporary arrays to hold intermediate results for appending.
    std::shared_ptr<common::DataChunkState> hashState;
    std::unique_ptr<common::ValueVector> hashVector;
    common::SelectionVector hashSelVec;
};

} // namespace processor
} // namespace kuzu
