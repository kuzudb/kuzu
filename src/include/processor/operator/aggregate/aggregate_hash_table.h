#pragma once

#include "function/aggregate/aggregate_function.h"
#include "function/comparison/comparison_operations.h"
#include "processor/operator/base_hash_table.h"
#include "storage/buffer_manager/memory_manager.h"

using namespace kuzu::function;

namespace kuzu {
namespace processor {

struct HashSlot {
    hash_t hash;    // 8 bytes for hashVector.
    uint8_t* entry; // pointer to the factorizedTable entry which stores [groupKey1, ...
                    // groupKeyN, aggregateState1, ..., aggregateStateN, hashValue].
};

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
using compare_function_t = std::function<bool(const uint8_t*, const uint8_t*)>;
using update_agg_function_t = std::function<void(AggregateHashTable*, const vector<ValueVector*>&,
    const vector<ValueVector*>&, unique_ptr<AggregateFunction>&, ValueVector*, uint64_t, uint32_t,
    uint32_t)>;

class AggregateHashTable : public BaseHashTable {
public:
    // Used by distinct aggregate hash table only.
    inline AggregateHashTable(MemoryManager& memoryManager,
        const vector<DataType>& groupByHashKeysDataTypes,
        const vector<unique_ptr<AggregateFunction>>& aggregateFunctions,
        uint64_t numEntriesToAllocate)
        : AggregateHashTable(memoryManager, groupByHashKeysDataTypes, vector<DataType>(),
              aggregateFunctions, numEntriesToAllocate) {}

    AggregateHashTable(MemoryManager& memoryManager, vector<DataType> groupByHashKeysDataTypes,
        vector<DataType> groupByNonHashKeysDataTypes,
        const vector<unique_ptr<AggregateFunction>>& aggregateFunctions,
        uint64_t numEntriesToAllocate);

    inline uint8_t* getEntry(uint64_t idx) { return factorizedTable->getTuple(idx); }

    inline FactorizedTable* getFactorizedTable() { return factorizedTable.get(); }

    inline uint64_t getNumEntries() const { return factorizedTable->getNumTuples(); }

    inline void append(const vector<ValueVector*>& groupByFlatKeyVectors,
        const vector<ValueVector*>& groupByUnFlatHashKeyVectors,
        const vector<ValueVector*>& aggregateVectors, uint64_t multiplicity) {
        append(groupByFlatKeyVectors, groupByUnFlatHashKeyVectors, vector<ValueVector*>(),
            aggregateVectors, multiplicity);
    }

    //! update aggregate states for an input
    void append(const vector<ValueVector*>& groupByFlatKeyVectors,
        const vector<ValueVector*>& groupByUnFlatHashKeyVectors,
        const vector<ValueVector*>& groupByNonHashKeyVectors,
        const vector<ValueVector*>& aggregateVectors, uint64_t multiplicity);

    bool isAggregateValueDistinctForGroupByKeys(
        const vector<ValueVector*>& groupByKeyVectors, ValueVector* aggregateVector);

    //! merge aggregate hash table by combining aggregate states under the same key
    void merge(AggregateHashTable& other);

    void finalizeAggregateStates();

    void resize(uint64_t newSize);

private:
    void initializeFT(const vector<unique_ptr<AggregateFunction>>& aggregateFunctions);

    void initializeHashTable(uint64_t numEntriesToAllocate);

    void initializeTmpVectors();

    // ! This function will only be used by distinct aggregate, which assumes that all groupByKeys
    // are flat.
    uint8_t* findEntryInDistinctHT(const vector<ValueVector*>& groupByKeyVectors, hash_t hash);

    void initializeFTEntryWithFlatVec(
        ValueVector* groupByFlatVector, uint64_t numEntriesToInitialize, uint32_t colIdx);

    void initializeFTEntryWithUnflatVec(
        ValueVector* groupByUnflatVector, uint64_t numEntriesToInitialize, uint32_t colIdx);

    void initializeFTEntries(const vector<ValueVector*>& groupByFlatHashKeyVectors,
        const vector<ValueVector*>& groupByUnflatHashKeyVectors,
        const vector<ValueVector*>& groupByNonHashKeyVectors, uint64_t numFTEntriesToInitialize);

    uint8_t* createEntryInDistinctHT(
        const vector<ValueVector*>& groupByHashKeyVectors, hash_t hash);

    void increaseSlotIdx(uint64_t& slotIdx) const;

    void initTmpHashSlotsAndIdxes();

    void increaseHashSlotIdxes(uint64_t numNoMatches);

    void findHashSlots(const vector<ValueVector*>& groupByFlatHashKeyVectors,
        const vector<ValueVector*>& groupByUnflatHashKeyVectors,
        const vector<ValueVector*>& groupByNonHashKeyVectors);

    void computeAndCombineVecHash(
        const vector<ValueVector*>& groupByUnflatHashKeyVectors, uint32_t startVecIdx);
    void computeVectorHashes(const vector<ValueVector*>& groupByFlatHashKeyVectors,
        const vector<ValueVector*>& groupByUnflatHashKeyVectors);

    void updateDistinctAggState(const vector<ValueVector*>& groupByFlatHashKeyVectors,
        const vector<ValueVector*>& groupByUnFlatHashKeyVectors,
        unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggregateVector,
        uint64_t multiplicity, uint32_t colIdx, uint32_t aggStateOffset);

    void updateAggState(const vector<ValueVector*>& groupByFlatHashKeyVectors,
        const vector<ValueVector*>& groupByUnFlatHashKeyVectors,
        unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector,
        uint64_t multiplicity, uint32_t colIdx, uint32_t aggStateOffset);

    void updateAggStates(const vector<ValueVector*>& groupByFlatHashKeyVectors,
        const vector<ValueVector*>& groupByUnFlatHashKeyVectors,
        const vector<ValueVector*>& aggregateVectors, uint64_t multiplicity);

    // ! This function will only be used by distinct aggregate, which assumes that all keyVectors
    // are flat.
    bool matchFlatGroupByKeys(const vector<ValueVector*>& keyVectors, uint8_t* entry);

    uint64_t matchUnflatVecWithFTColumn(
        ValueVector* vector, uint64_t numMayMatches, uint64_t& numNoMatches, uint32_t colIdx);

    uint64_t matchFlatVecWithFTColumn(
        ValueVector* vector, uint64_t numMayMatches, uint64_t& numNoMatches, uint32_t colIdx);

    uint64_t matchFTEntries(const vector<ValueVector*>& groupByFlatHashKeyVectors,
        const vector<ValueVector*>& groupByUnflatHashKeyVectors,
        const vector<ValueVector*>& groupByNonHashKeyVectors, uint64_t numMayMatches,
        uint64_t numNoMatches);

    void fillEntryWithInitialNullAggregateState(uint8_t* entry);

    //! find an uninitialized hash slot for given hash and fill hash slot with block id and offset
    void fillHashSlot(hash_t hash, uint8_t* groupByKeysAndAggregateStateBuffer);

    inline HashSlot* getHashSlot(uint64_t slotIdx) {
        assert(slotIdx < maxNumHashSlots);
        // If the slotIdx is smaller than the numHashSlotsPerBlock, then the hashSlot must be
        // in the first hashSlotsBlock. We don't need to compute the blockIdx and blockOffset.
        return slotIdx < ((uint64_t)1 << numSlotsPerBlockLog2) ?
                   (HashSlot*)(hashSlotsBlocks[0]->getData() + slotIdx * sizeof(HashSlot)) :
                   (HashSlot*)(hashSlotsBlocks[slotIdx >> numSlotsPerBlockLog2]->getData() +
                               (slotIdx & slotIdxInBlockMask) * sizeof(HashSlot));
    }

    void addDataBlocksIfNecessary(uint64_t maxNumHashSlots);

    void resizeHashTableIfNecessary(uint32_t maxNumDistinctHashKeys);

    template<typename type>
    static bool compareEntryWithKeys(const uint8_t* keyValue, const uint8_t* entry);

    static compare_function_t getCompareEntryWithKeysFunc(DataTypeID typeId);

    void updateNullAggVectorState(const vector<ValueVector*>& groupByFlatHashKeyVectors,
        const vector<ValueVector*>& groupByUnflatHashKeyVectors,
        unique_ptr<AggregateFunction>& aggregateFunction, uint64_t multiplicity,
        uint32_t aggStateOffset);

    void updateBothFlatAggVectorState(const vector<ValueVector*>& groupByFlatHashKeyVectors,
        unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector,
        uint64_t multiplicity, uint32_t aggStateOffset);

    void updateFlatUnflatKeyFlatAggVectorState(
        const vector<ValueVector*>& groupByFlatHashKeyVectors,
        const vector<ValueVector*>& groupByUnflatHashKeyVectors,
        unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector,
        uint64_t multiplicity, uint32_t aggStateOffset);

    void updateFlatKeyUnflatAggVectorState(const vector<ValueVector*>& groupByFlatHashKeyVectors,
        unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector,
        uint64_t multiplicity, uint32_t aggStateOffset);

    void updateBothUnflatSameDCAggVectorState(const vector<ValueVector*>& groupByFlatHashKeyVectors,
        const vector<ValueVector*>& groupByUnflatHashKeyVectors,
        unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector,
        uint64_t multiplicity, uint32_t aggStateOffset);

    void updateBothUnflatDifferentDCAggVectorState(
        const vector<ValueVector*>& groupByFlatHashKeyVectors,
        const vector<ValueVector*>& groupByUnflatHashKeyVectors,
        unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector,
        uint64_t multiplicity, uint32_t aggStateOffset);

private:
    vector<DataType> groupByHashKeysDataTypes;
    vector<DataType> groupByNonHashKeysDataTypes;
    vector<unique_ptr<AggregateFunction>> aggregateFunctions;

    //! special handling of distinct aggregate
    vector<unique_ptr<AggregateHashTable>> distinctHashTables;
    uint32_t hashColIdxInFT;
    uint32_t hashColOffsetInFT;
    uint32_t aggStateColOffsetInFT;
    uint32_t aggStateColIdxInFT;
    uint32_t numBytesForGroupByHashKeys = 0;
    uint32_t numBytesForGroupByNonHashKeys = 0;
    vector<compare_function_t> compareFuncs;
    vector<update_agg_function_t> updateAggFuncs;
    bool hasStrCol = false;
    // Temporary arrays to hold intermediate results.
    shared_ptr<DataChunkState> hashState;
    shared_ptr<ValueVector> hashVector;
    unique_ptr<HashSlot*[]> hashSlotsToUpdateAggState;
    unique_ptr<uint64_t[]> tmpValueIdxes;
    unique_ptr<uint64_t[]> entryIdxesToInitialize;
    unique_ptr<uint64_t[]> mayMatchIdxes;
    unique_ptr<uint64_t[]> noMatchIdxes;
    unique_ptr<uint64_t[]> tmpSlotIdxes;
};

class AggregateHashTableUtils {

public:
    static vector<unique_ptr<AggregateHashTable>> createDistinctHashTables(
        MemoryManager& memoryManager, const vector<DataType>& groupByKeyDataTypes,
        const vector<unique_ptr<AggregateFunction>>& aggregateFunctions);
};

} // namespace processor
} // namespace kuzu
