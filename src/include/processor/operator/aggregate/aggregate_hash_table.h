#pragma once

#include "aggregate_input.h"
#include "function/aggregate_function.h"
#include "processor/result/base_hash_table.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace processor {

struct HashSlot {
    common::hash_t hash; // 8 bytes for hashVector.
    uint8_t* entry;      // pointer to the factorizedTable entry which stores [groupKey1, ...
                         // groupKeyN, aggregateState1, ..., aggregateStateN, hashValue].
};

enum class HashTableType : uint8_t { AGGREGATE_HASH_TABLE = 0, MARK_HASH_TABLE = 1 };

/**
 * AggregateHashTable Design
 *
 * 1. Payload
 * Entry layout: [groupKey1, ... groupKeyN, aggregateState1, ..., aggregateStateN, hashValue]
 * Payload is stored in the factorizedTable.
 *
 * 2. Hash slot
 * Layout : see HashSlot struct
 * If the entry is a nullptr, then the current hashSlot is unused.
 *
 * 3. Collision handling
 * Linear probing. When collision happens, we find the next hash slot whose entry is a
 * nullptr.
 *
 */
class AggregateHashTable;
using update_agg_function_t =
    std::function<void(AggregateHashTable*, const std::vector<common::ValueVector*>&,
        const std::vector<common::ValueVector*>&, std::unique_ptr<function::AggregateFunction>&,
        common::ValueVector*, uint64_t, uint32_t, uint32_t)>;

class AggregateHashTable : public BaseHashTable {
public:
    AggregateHashTable(storage::MemoryManager& memoryManager,
        const std::vector<common::LogicalType>& keyTypes,
        const std::vector<common::LogicalType>& payloadTypes, uint64_t numEntriesToAllocate,
        FactorizedTableSchema tableSchema)
        : AggregateHashTable(memoryManager, common::LogicalType::copy(keyTypes),
              common::LogicalType::copy(payloadTypes),
              std::vector<std::unique_ptr<function::AggregateFunction>>{} /* empty aggregates */,
              std::vector<common::LogicalType>{} /* empty distinct agg key*/, numEntriesToAllocate,
              std::move(tableSchema)) {}

    AggregateHashTable(storage::MemoryManager& memoryManager,
        std::vector<common::LogicalType> keyTypes, std::vector<common::LogicalType> payloadTypes,
        const std::vector<std::unique_ptr<function::AggregateFunction>>& aggregateFunctions,
        const std::vector<common::LogicalType>& distinctAggKeyTypes, uint64_t numEntriesToAllocate,
        FactorizedTableSchema tableSchema);

    uint8_t* getEntry(uint64_t idx) { return factorizedTable->getTuple(idx); }

    FactorizedTable* getFactorizedTable() { return factorizedTable.get(); }

    uint64_t getNumEntries() const { return factorizedTable->getNumTuples(); }

    void append(const std::vector<common::ValueVector*>& flatKeyVectors,
        const std::vector<common::ValueVector*>& unFlatKeyVectors,
        common::DataChunkState* leadingState, const std::vector<AggregateInput>& aggregateInputs,
        uint64_t resultSetMultiplicity) {
        append(flatKeyVectors, unFlatKeyVectors, std::vector<common::ValueVector*>(), leadingState,
            aggregateInputs, resultSetMultiplicity);
    }

    //! update aggregate states for an input
    void append(const std::vector<common::ValueVector*>& flatKeyVectors,
        const std::vector<common::ValueVector*>& unFlatKeyVectors,
        const std::vector<common::ValueVector*>& dependentKeyVectors,
        common::DataChunkState* leadingState, const std::vector<AggregateInput>& aggregateInputs,
        uint64_t resultSetMultiplicity);

    bool isAggregateValueDistinctForGroupByKeys(
        const std::vector<common::ValueVector*>& groupByKeyVectors,
        common::ValueVector* aggregateVector);

    //! merge aggregate hash table by combining aggregate states under the same key
    void merge(AggregateHashTable& other);

    void finalizeAggregateStates();

    void resize(uint64_t newSize);

protected:
    virtual uint64_t matchFTEntries(const std::vector<common::ValueVector*>& flatKeyVectors,
        const std::vector<common::ValueVector*>& unFlatKeyVectors, uint64_t numMayMatches,
        uint64_t numNoMatches);

    virtual void initializeFTEntries(const std::vector<common::ValueVector*>& flatKeyVectors,
        const std::vector<common::ValueVector*>& unFlatKeyVectors,
        const std::vector<common::ValueVector*>& dependentKeyVectors,
        uint64_t numFTEntriesToInitialize);

    uint64_t matchUnFlatVecWithFTColumn(common::ValueVector* vector, uint64_t numMayMatches,
        uint64_t& numNoMatches, uint32_t colIdx);

    uint64_t matchFlatVecWithFTColumn(common::ValueVector* vector, uint64_t numMayMatches,
        uint64_t& numNoMatches, uint32_t colIdx);

private:
    void initializeFT(
        const std::vector<std::unique_ptr<function::AggregateFunction>>& aggregateFunctions,
        FactorizedTableSchema tableSchema);

    void initializeHashTable(uint64_t numEntriesToAllocate);

    void initializeTmpVectors();

    // ! This function will only be used by distinct aggregate, which assumes that all groupByKeys
    // are flat.
    uint8_t* findEntryInDistinctHT(const std::vector<common::ValueVector*>& groupByKeyVectors,
        common::hash_t hash);

    void initializeFTEntryWithFlatVec(common::ValueVector* flatVector,
        uint64_t numEntriesToInitialize, uint32_t colIdx);

    void initializeFTEntryWithUnFlatVec(common::ValueVector* unFlatVector,
        uint64_t numEntriesToInitialize, uint32_t colIdx);

    uint8_t* createEntryInDistinctHT(const std::vector<common::ValueVector*>& groupByHashKeyVectors,
        common::hash_t hash);

    void increaseSlotIdx(uint64_t& slotIdx) const;

    void initTmpHashSlotsAndIdxes();

    void increaseHashSlotIdxes(uint64_t numNoMatches);

    void findHashSlots(const std::vector<common::ValueVector*>& flatKeyVectors,
        const std::vector<common::ValueVector*>& unFlatKeyVectors,
        const std::vector<common::ValueVector*>& dependentKeyVectors,
        common::DataChunkState* leadingState);

    void updateDistinctAggState(const std::vector<common::ValueVector*>& flatKeyVectors,
        const std::vector<common::ValueVector*>& unFlatKeyVectors,
        std::unique_ptr<function::AggregateFunction>& aggregateFunction,
        common::ValueVector* aggregateVector, uint64_t multiplicity, uint32_t colIdx,
        uint32_t aggStateOffset);

    void updateAggState(const std::vector<common::ValueVector*>& flatKeyVectors,
        const std::vector<common::ValueVector*>& unFlatKeyVectors,
        std::unique_ptr<function::AggregateFunction>& aggregateFunction,
        common::ValueVector* aggVector, uint64_t multiplicity, uint32_t colIdx,
        uint32_t aggStateOffset);

    void updateAggStates(const std::vector<common::ValueVector*>& flatKeyVectors,
        const std::vector<common::ValueVector*>& unFlatKeyVectors,
        const std::vector<AggregateInput>& aggregateInputs, uint64_t resultSetMultiplicity);

    void fillEntryWithInitialNullAggregateState(uint8_t* entry);

    //! find an uninitialized hash slot for given hash and fill hash slot with block id and offset
    void fillHashSlot(common::hash_t hash, uint8_t* groupByKeysAndAggregateStateBuffer);

    inline HashSlot* getHashSlot(uint64_t slotIdx) {
        KU_ASSERT(slotIdx < maxNumHashSlots);
        // If the slotIdx is smaller than the numHashSlotsPerBlock, then the hashSlot must be
        // in the first hashSlotsBlock. We don't need to compute the blockIdx and blockOffset.
        return slotIdx < ((uint64_t)1 << numSlotsPerBlockLog2) ?
                   (HashSlot*)(hashSlotsBlocks[0]->getData() + slotIdx * sizeof(HashSlot)) :
                   (HashSlot*)(hashSlotsBlocks[slotIdx >> numSlotsPerBlockLog2]->getData() +
                               (slotIdx & slotIdxInBlockMask) * sizeof(HashSlot));
    }

    void addDataBlocksIfNecessary(uint64_t maxNumHashSlots);

    void resizeHashTableIfNecessary(uint32_t maxNumDistinctHashKeys);

    void updateNullAggVectorState(const std::vector<common::ValueVector*>& flatKeyVectors,
        const std::vector<common::ValueVector*>& unFlatKeyVectors,
        std::unique_ptr<function::AggregateFunction>& aggregateFunction, uint64_t multiplicity,
        uint32_t aggStateOffset);

    void updateBothFlatAggVectorState(const std::vector<common::ValueVector*>& flatKeyVectors,
        std::unique_ptr<function::AggregateFunction>& aggregateFunction,
        common::ValueVector* aggVector, uint64_t multiplicity, uint32_t aggStateOffset);

    void updateFlatUnFlatKeyFlatAggVectorState(
        const std::vector<common::ValueVector*>& flatKeyVectors,
        const std::vector<common::ValueVector*>& unFlatKeyVectors,
        std::unique_ptr<function::AggregateFunction>& aggregateFunction,
        common::ValueVector* aggVector, uint64_t multiplicity, uint32_t aggStateOffset);

    void updateFlatKeyUnFlatAggVectorState(const std::vector<common::ValueVector*>& flatKeyVectors,
        std::unique_ptr<function::AggregateFunction>& aggregateFunction,
        common::ValueVector* aggVector, uint64_t multiplicity, uint32_t aggStateOffset);

    void updateBothUnFlatSameDCAggVectorState(
        const std::vector<common::ValueVector*>& flatKeyVectors,
        const std::vector<common::ValueVector*>& unFlatKeyVectors,
        std::unique_ptr<function::AggregateFunction>& aggregateFunction,
        common::ValueVector* aggVector, uint64_t multiplicity, uint32_t aggStateOffset);

    void updateBothUnFlatDifferentDCAggVectorState(
        const std::vector<common::ValueVector*>& flatKeyVectors,
        const std::vector<common::ValueVector*>& unFlatKeyVectors,
        std::unique_ptr<function::AggregateFunction>& aggregateFunction,
        common::ValueVector* aggVector, uint64_t multiplicity, uint32_t aggStateOffset);

protected:
    uint32_t hashColIdxInFT;
    std::unique_ptr<uint64_t[]> mayMatchIdxes;
    std::unique_ptr<uint64_t[]> noMatchIdxes;
    std::unique_ptr<uint64_t[]> entryIdxesToInitialize;
    std::unique_ptr<HashSlot*[]> hashSlotsToUpdateAggState;

private:
    std::vector<common::LogicalType> payloadTypes;
    std::vector<std::unique_ptr<function::AggregateFunction>> aggregateFunctions;

    //! special handling of distinct aggregate
    std::vector<std::unique_ptr<AggregateHashTable>> distinctHashTables;
    uint32_t hashColOffsetInFT;
    uint32_t aggStateColOffsetInFT;
    uint32_t aggStateColIdxInFT;
    uint32_t numBytesForKeys = 0;
    uint32_t numBytesForDependentKeys = 0;
    std::vector<update_agg_function_t> updateAggFuncs;
    // Temporary arrays to hold intermediate results.
    std::unique_ptr<uint64_t[]> tmpValueIdxes;
    std::unique_ptr<uint64_t[]> tmpSlotIdxes;
};

struct AggregateHashTableUtils {
    static std::unique_ptr<AggregateHashTable> createDistinctHashTable(
        storage::MemoryManager& memoryManager,
        const std::vector<common::LogicalType>& groupByKeyTypes,
        const common::LogicalType& distinctKeyType);
};

} // namespace processor
} // namespace kuzu
