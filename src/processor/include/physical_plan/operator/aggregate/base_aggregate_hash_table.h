#pragma once

#include "src/common/include/memory_manager.h"
#include "src/function/include/aggregate/aggregate_function.h"

using namespace graphflow::function;

namespace graphflow {
namespace processor {

constexpr uint32_t EMPTY_BLOCK_ID = 0; // Valid block id starts from 1
constexpr uint16_t HASH_PREFIX_SHIFT = (sizeof(hash_t) - sizeof(uint16_t)) * 8;

struct HashSlot {
    uint16_t hashPrefix;    // 16 high bits of the hash value for fast comparison.
    uint16_t offsetInBlock; // i-th entry in a block.
    uint32_t blockId;       // i-th block in the blocks array.
};

struct EntryCursor {
    uint32_t entryBlockId;
    uint16_t offsetInBlock;
};

/**
 * AggregateHashTable Design
 *
 * 1. Payload
 * Entry layout: [hash, groupKey1, ... groupKeyN, aggregateState1, ..., aggregateStateN]
 * Payload memory block 0 is created as empty memory block because hash slot used block ID 0 to
 * indicate uninitialized hash slot.
 * Payload memory blocks is allocated on demand, i.e. allocated when current block is full.
 *
 * 2. Hash slot
 * Layout : see HashSlot struct
 * Block ID 0 indicates a HashSlot is uninitialized.
 *
 * 3. Collision handling
 * Linear probing. When collision happens, we find the next hash slot whose block ID is not 0.
 *
 */
class AggregateHashTable {

public:
    AggregateHashTable(MemoryManager& memoryManager, vector<DataType> groupByKeysDataTypes,
        const vector<unique_ptr<AggregateFunction>>& aggregateFunctions,
        uint64_t numEntriesToAllocate);

    inline uint64_t getNumEntries() const { return numEntries; }

    inline vector<DataType> getGroupByKeysDataTypes() const { return groupByKeysDataTypes; }

    uint8_t* getEntry(uint64_t id);

    //! update aggregate states for an input
    void append(const vector<ValueVector*>& groupByKeyVectors,
        const vector<ValueVector*>& aggregateVectors, uint64_t multiplicity);

    //! merge aggregate hash table by combining aggregate states under the same key
    void merge(AggregateHashTable& other);

    void finalizeAggregateStates();

private:
    uint8_t* findEntry(const vector<ValueVector*>& groupByKeyVectors, hash_t hash);

    uint8_t* findEntry(uint8_t* groupByKeys, hash_t hash);

    uint8_t* createEntry(const vector<ValueVector*>& groupByKeyVectors, hash_t hash);

    uint8_t* createEntry(uint8_t* groupByKeys, hash_t hash);

    inline uint64_t getNumBytesForHash() const { return sizeof(hash_t); }

    uint64_t getNumBytesForGroupByKeys() const;

    uint64_t getNumBytesForAggregateStates() const;

    inline uint64_t getNumBytesForEntry() const {
        return getNumBytesForHash() + getNumBytesForGroupByKeys() + getNumBytesForAggregateStates();
    }

    inline uint64_t getGroupByKeysOffsetInEntry() const { return getNumBytesForHash(); }

    inline uint64_t getAggregateStatesOffsetInEntry() const {
        return getGroupByKeysOffsetInEntry() + getNumBytesForGroupByKeys();
    }

    inline uint8_t* getEntry(uint32_t entryBlockId, uint16_t offsetInBlock) const {
        return entryBlocks[entryBlockId]->data + getNumBytesForEntry() * offsetInBlock;
    }

    inline bool isCurrentEntryFull() const {
        return currentEntryCursor.offsetInBlock == numEntriesPerBlock;
    }

    inline uint8_t* getCurrentEntry() const {
        return getEntry(currentEntryCursor.entryBlockId, currentEntryCursor.offsetInBlock);
    }

    void increaseSlotOffset(uint64_t& slotOffset) const;

    //! compute hash for multiple flat key vectors
    hash_t computeHash(const vector<ValueVector*>& keyVectors);

    //! compute hash for single flat key vector
    hash_t computeHash(ValueVector* keyVector);

    //! compute hash for multiple keys
    hash_t computeHash(uint8_t* keys);

    //! compute hash for single key value
    hash_t computeHash(DataType keyDataType, uint8_t* keyValue);

    //! check if flat key vectors are the same as keys stored in entry
    bool matchGroupByKeys(const vector<ValueVector*>& keyVectors, uint8_t* entry);

    //! check if flat key vector is the same as value
    bool matchGroupByKey(ValueVector* keyVector, uint8_t* value);

    //! check if keys are the same as keys stored in entry.
    bool matchGroupByKeys(uint8_t* keys, uint8_t* entry);

    void fillEntryWithHash(uint8_t* entry, hash_t hash);

    void fillEntryWithInitialNullAggregateState(uint8_t* entry);

    //! find an uninitialized hash slot for given hash and fill hash slot with block id and offset
    void fillHashSlot(hash_t hash, uint32_t blockId, uint16_t offsetInBlock);

    void addNewBlock();

    void resize(uint64_t newSize);

private:
    MemoryManager& memoryManager;
    vector<DataType> groupByKeysDataTypes;
    vector<unique_ptr<AggregateFunction>> aggregateFunctions;

    // this number should be power of 2, otherwise we will have too many collision
    uint64_t maxNumHashSlots;

    //! entry related fields
    uint64_t numEntries;
    EntryCursor currentEntryCursor;
    uint64_t numEntriesPerBlock;
    vector<unique_ptr<MemoryBlock>> entryBlocks;

    //! hash related fields
    uint64_t bitmask;
    unique_ptr<MemoryBlock> hashSlotsBlock;
    HashSlot* hashSlots;
};

} // namespace processor
} // namespace graphflow
