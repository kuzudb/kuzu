#pragma once

#include <functional>

#include "processor/operator/base_hash_table.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace processor {

class JoinHashTable : public BaseHashTable {
    using hash_function_t = std::function<void(const uint8_t*, common::hash_t&)>;
    using compare_function_t =
        std::function<void(const common::ValueVector&, uint32_t pos, const uint8_t*, uint8_t&)>;

public:
    JoinHashTable(storage::MemoryManager& memoryManager,
        std::vector<std::unique_ptr<common::LogicalType>> keyTypes,
        std::unique_ptr<FactorizedTableSchema> tableSchema);

    void appendVectors(const std::vector<common::ValueVector*>& keyVectors,
        const std::vector<common::ValueVector*>& payloadVectors, common::DataChunkState* keyState);
    void appendVector(common::ValueVector* vector,
        const std::vector<BlockAppendingInfo>& appendInfos, ft_col_idx_t colIdx);

    // Used in worst-case optimal join
    void appendVectorWithSorting(
        common::ValueVector* keyVector, std::vector<common::ValueVector*> payloadVectors);

    void allocateHashSlots(uint64_t numTuples);
    void buildHashSlots();

    void probe(const std::vector<common::ValueVector*>& keyVectors, common::ValueVector* hashVector,
        common::ValueVector* tmpHashVector, uint8_t** probedTuples);
    // All key vectors must be flat. Thus input is a tuple, multiple matches can be found for the
    // given key tuple.
    common::sel_t matchFlatKeys(const std::vector<common::ValueVector*>& keyVectors,
        uint8_t** probedTuples, uint8_t** matchedTuples);
    // Input is multiple tuples, at most one match exist for each key.
    common::sel_t matchUnFlatKey(common::ValueVector* keyVector, uint8_t** probedTuples,
        uint8_t** matchedTuples, common::SelectionVector* matchedTuplesSelVector);

    inline void lookup(std::vector<common::ValueVector*>& vectors,
        std::vector<uint32_t>& colIdxesToScan, uint8_t** tuplesToRead, uint64_t startPos,
        uint64_t numTuplesToRead) {
        factorizedTable->lookup(vectors, colIdxesToScan, tuplesToRead, startPos, numTuplesToRead);
    }
    inline void merge(JoinHashTable& other) { factorizedTable->merge(*other.factorizedTable); }
    inline uint64_t getNumTuples() { return factorizedTable->getNumTuples(); }
    inline uint8_t** getPrevTuple(const uint8_t* tuple) const {
        return (uint8_t**)(tuple + prevPtrColOffset);
    }
    inline uint8_t* getTupleForHash(common::hash_t hash) {
        auto slotIdx = getSlotIdxForHash(hash);
        return ((uint8_t**)(hashSlotsBlocks[slotIdx >> numSlotsPerBlockLog2]
                                ->getData()))[slotIdx & slotIdxInBlockMask];
    }
    inline FactorizedTable* getFactorizedTable() { return factorizedTable.get(); }
    inline const FactorizedTableSchema* getTableSchema() {
        return factorizedTable->getTableSchema();
    }

private:
    uint8_t** findHashSlot(uint8_t* tuple) const;
    // This function returns the pointer that previously stored in the same slot.
    uint8_t* insertEntry(uint8_t* tuple) const;

    bool compareFlatKeys(const std::vector<common::ValueVector*>& keyVectors, const uint8_t* tuple);

    void initFunctions();
    void getHashFunction(common::PhysicalTypeID physicalTypeID, hash_function_t& func);
    void getCompareFunction(common::PhysicalTypeID physicalTypeID, compare_function_t& func);

private:
    std::vector<std::unique_ptr<common::LogicalType>> keyTypes;
    std::vector<hash_function_t> entryHashFunctions;
    std::vector<compare_function_t> entryCompareFunctions;

    const FactorizedTableSchema* tableSchema;
    uint64_t prevPtrColOffset;
};

} // namespace processor
} // namespace kuzu
