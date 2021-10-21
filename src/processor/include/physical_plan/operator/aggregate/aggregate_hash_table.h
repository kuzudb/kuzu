#pragma once

#include "src/common/include/memory_manager.h"
#include "src/expression_evaluator/include/aggregate_expression_evaluator.h"
#include "src/processor/include/physical_plan/data_pos.h"
#include "src/processor/include/physical_plan/operator/result/result_set.h"

using namespace graphflow::evaluator;

namespace graphflow {
namespace processor {

constexpr uint32_t EMPTY_BLOCK_ID = 0; // Valid block id starts from 1
constexpr uint16_t HASH_PREFIX_SHIFT = (sizeof(hash_t) - sizeof(uint16_t)) * 8;

struct HashSlot {
    uint16_t hashPrefix;    // 16 high bits of the hash value for fast comparison.
    uint16_t offsetInBlock; // i-th entry in a block.
    uint32_t blockId;       // i-th block in the blocks array.
};

//! The hash table uses linear probing for collision resolution.
class AggregateHashTable {
public:
    AggregateHashTable(MemoryManager& memoryManager,
        vector<unique_ptr<AggregateExpressionEvaluator>> aggregates, uint64_t groupsSizeInEntry,
        uint64_t numBytesPerEntry);

    void append(const vector<shared_ptr<ValueVector>>& groupVectors,
        const vector<shared_ptr<ValueVector>>& aggregateVectors);
    uint8_t* lookup(const vector<shared_ptr<ValueVector>>& groupVectors);

private:
    void resize(uint64_t newSize);
    uint8_t* findGroup(const vector<shared_ptr<ValueVector>>& groupVectors);
    uint8_t* createNewGroup(const vector<shared_ptr<ValueVector>>& groupVectors);
    void addNewBlock();
    void updateHashSlot(hash_t hash, uint32_t blockId, uint16_t offsetInBlock) const;
    bool matchGroups(const vector<shared_ptr<ValueVector>>& groupVectors, uint8_t* entry);
    void computeHashesForVectors(const vector<shared_ptr<ValueVector>>& vectors) const;

private:
    MemoryManager& memoryManager;
    vector<unique_ptr<AggregateExpressionEvaluator>> aggregates;
    uint64_t numBytesPerEntry; // Entry: [hash, group key, ..., aggregate state, ...]
    uint64_t numBytesForGroupKeys;
    uint64_t numGroups;
    uint64_t capacity; // Max num of slots in current hashSlotBlock.
    uint64_t bitmask;
    uint64_t numEntriesPerBlock;
    uint64_t offsetInCurrentBlock;
    vector<unique_ptr<MemoryBlock>> blocks; // Blocks for hash table entries
    unique_ptr<MemoryBlock> hashSlotsBlock; // The block for hash slots
    HashSlot* hashSlots;

    vector<unique_ptr<AggregationState>> initializedStates;
    unique_ptr<ValueVector> groupHashes;
};
} // namespace processor
} // namespace graphflow
