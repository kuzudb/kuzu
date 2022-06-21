#pragma once

#include "src/function/aggregate/include/aggregate_function.h"
#include "src/processor/include/physical_plan/hash_table/base_hash_table.h"
#include "src/storage/buffer_manager/include/memory_manager.h"

using namespace graphflow::function;

namespace graphflow {
namespace processor {

struct HashSlot {
    hash_t hash;    // 8 bytes for hashValues.
    uint8_t* entry; // pointer to the tuple buffer which stores [groupKey1, ...
                    // groupKeyN, aggregateState1, ..., aggregateStateN].
};

/**
 * AggregateHashTable Design
 *
 * 1. Payload
 * Entry layout: [groupKey1, ... groupKeyN, aggregateState1, ..., aggregateStateN]
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

    inline uint64_t getNumEntries() const { return factorizedTable->getNumTuples(); }

    inline vector<DataType> getGroupByHashKeysDataTypes() const { return groupByHashKeysDataTypes; }

    inline vector<DataType> getGroupByNonHashKeysDataTypes() const {
        return groupByNonHashKeysDataTypes;
    }

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

    inline uint8_t* getEntry(uint64_t idx) { return factorizedTable->getTuple(idx); }

    inline FactorizedTable* getFactorizedTable() { return factorizedTable.get(); }

private:
    uint8_t* findEntry(uint8_t* entryBuffer, hash_t hash);

    // ! This function will only be used by distinct aggregate, which assumes that all groupByKeys
    // are flat.
    uint8_t* findEntry(const vector<ValueVector*>& groupByKeyVectors, hash_t hash);

    uint8_t* createEntry(const vector<ValueVector*>& groupByFlatHashKeyVectors,
        const vector<ValueVector*>& groupByUnflatHashKeyVectors,
        const vector<ValueVector*>& groupByNonKeyVectors, hash_t hash, uint32_t unflatVectorIdx,
        HashSlot* slot);

    uint8_t* createEntry(uint8_t* groupByKeys, hash_t hash);

    uint8_t* createEntry(const vector<ValueVector*>& groupByHashKeyVectors, hash_t hash);

    uint64_t getNumBytesForGroupByHashKeys() const;

    uint64_t getNumBytesForGroupByNonHashKeys() const;

    inline uint64_t getAggregateStatesOffsetInFT() const {
        return getNumBytesForGroupByHashKeys() + getNumBytesForGroupByNonHashKeys();
    }

    void increaseSlotIdx(uint64_t& slotIdx) const;

    void findHashSlots(const vector<ValueVector*>& groupByFlatHashKeyVectors,
        const vector<ValueVector*>& groupByUnFlatHashKeyVectors,
        const vector<ValueVector*>& groupByNonHashKeyVectors);

    static hash_t computeHash(ValueVector* keyVector, uint32_t pos);
    static hash_t computeFlatVecHash(const vector<ValueVector*>& groupByFlatHashKeyVectors);
    void computeUnflatVecHash(const vector<ValueVector*>& groupByUnflatHashKeyVectors,
        uint32_t startVecIdx, hash_t initHashVal);
    void computeVectorHashes(const vector<ValueVector*>& groupByFlatHashKeyVectors,
        const vector<ValueVector*>& groupByUnflatHashKeyVectors);

    //! compute hash for multiple keys
    hash_t computeHash(uint8_t* keys);

    //! compute hash for single key value
    hash_t computeHash(const DataType& keyDataType, uint8_t* keyValue, uint64_t colIdx);

    void updateAggStates(const vector<ValueVector*>& groupByFlatHashKeyVectors,
        const vector<ValueVector*>& groupByUnFlatHashKeyVectors,
        const vector<ValueVector*>& aggregateVectors, uint64_t multiplicity);

    // ! This function will only be used by distinct aggregate, which assumes that all keyVectors
    // are flat.
    bool matchFlatGroupByKeys(const vector<ValueVector*>& keyVectors, uint8_t* entry);

    bool matchGroupByKey(ValueVector* keyVector, uint8_t* entry, uint32_t pos, uint32_t colIdx);

    bool matchGroupByKeys(const vector<ValueVector*>& groupByFlatHashKeyVectors,
        const vector<ValueVector*>& groupByUnflatHashKeyVectors, uint8_t* entry,
        uint32_t unflatVectorIdx);

    //! check if keys are the same as keys stored in entryBufferToMatch.
    bool matchGroupByKeys(uint8_t* keys, uint8_t* entryBufferToMatch);

    void fillTupleWithInitialNullAggregateState(uint8_t* tuple);

    //! find an uninitialized hash slot for given hash and fill hash slot with block id and offset
    void fillHashSlot(hash_t hash, uint8_t* groupByKeysAndAggregateStateBuffer);

    void resize(uint64_t newSize);

    inline HashSlot* getHashSlot(uint64_t slotIdx) {
        assert(slotIdx < maxNumHashSlots);
        return (HashSlot*)(hashSlotsBlocks[slotIdx / numHashSlotsPerBlock]->getData() +
                           slotIdx % numHashSlotsPerBlock * sizeof(HashSlot));
    }

    void addDataBlocksIfNecessary(uint64_t maxNumHashSlots);

    inline void fillTupleWithGroupByKeys(uint8_t* tupleBuffer, uint8_t* groupByKeys) {
        memcpy(tupleBuffer, groupByKeys,
            getNumBytesForGroupByHashKeys() + getNumBytesForGroupByNonHashKeys());
    }

    inline void fillTupleWithNullMap(uint8_t* entryNullBuffer, uint8_t* groupByKeyNullBuffer) {
        memcpy(entryNullBuffer, groupByKeyNullBuffer,
            factorizedTable->getTableSchema().getNumBytesForNullMap());
    }

    static bool compareEntryWithKeys(
        bool isStrColumn, uint8_t* keyValue, uint8_t* entry, uint32_t numBytesToCompare);

    void resizeHashTableIfNecessary();

private:
    vector<DataType> groupByHashKeysDataTypes;
    vector<DataType> groupByNonHashKeysDataTypes;
    vector<unique_ptr<AggregateFunction>> aggregateFunctions;

    //! special handling of distinct aggregate
    vector<unique_ptr<AggregateHashTable>> distinctHashTables;

    bool hasStrCol = false;
    unique_ptr<hash_t[]> hashValues;
    unique_ptr<hash_t[]> tmpHashValues;
    unique_ptr<HashSlot*[]> hashSlots;
    uint32_t aggStateColOffsetInFT;
    uint32_t aggStateColIdxInFT;
};

class AggregateHashTableUtils {

public:
    static vector<unique_ptr<AggregateHashTable>> createDistinctHashTables(
        MemoryManager& memoryManager, const vector<DataType>& groupByKeyDataTypes,
        const vector<unique_ptr<AggregateFunction>>& aggregateFunctions);
};

} // namespace processor
} // namespace graphflow
