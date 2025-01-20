#pragma once

#include <functional>

#include "common/copy_constructors.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "processor/result/factorized_table.h"
#include "processor/result/factorized_table_schema.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace processor {

using compare_function_t = std::function<bool(common::ValueVector*, uint32_t, const uint8_t*)>;
using raw_compare_function_t = std::function<bool(const uint8_t*, const uint8_t*)>;

class BaseHashTable {
public:
    BaseHashTable(storage::MemoryManager& memoryManager, common::logical_type_vec_t keyTypes);

    virtual ~BaseHashTable() = default;

    DELETE_COPY_DEFAULT_MOVE(BaseHashTable);

    const FactorizedTableSchema* getTableSchema() const {
        return factorizedTable->getTableSchema();
    }
    uint64_t getNumEntries() const { return factorizedTable->getNumTuples(); }
    const FactorizedTable* getFactorizedTable() const { return factorizedTable.get(); }

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
    storage::MemoryManager* memoryManager;
    std::unique_ptr<FactorizedTable> factorizedTable;
    std::vector<compare_function_t> compareEntryFuncs;
    std::vector<raw_compare_function_t> rawCompareEntryFuncs;
    std::vector<common::LogicalType> keyTypes;
    // Temporary arrays to hold intermediate results for appending.
    std::shared_ptr<common::DataChunkState> hashState;
    std::unique_ptr<common::ValueVector> hashVector;
    common::SelectionVector hashSelVec;
    std::unique_ptr<common::ValueVector> tmpHashResultVector;
    std::unique_ptr<common::ValueVector> tmpHashCombineResultVector;
};

} // namespace processor
} // namespace kuzu
