#include "src/processor/include/physical_plan/operator/aggregate/aggregate_hash_table.h"

#include "src/common/include/vector/operations/vector_hash_operations.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

AggregateHashTable::AggregateHashTable(MemoryManager& memoryManager,
    vector<unique_ptr<AggregateFunction>> aggregates, uint64_t groupsSizeInEntry,
    uint64_t numBytesPerEntry)
    : memoryManager{memoryManager}, aggregates{move(aggregates)},
      numBytesPerEntry{numBytesPerEntry}, numBytesForGroupKeys{groupsSizeInEntry}, numGroups{0},
      offsetInCurrentBlock{0} {
    numEntriesPerBlock = DEFAULT_MEMORY_BLOCK_SIZE / numBytesPerEntry;
    for (auto& aggregate : this->aggregates) {
        initializedStates.push_back(aggregate->initialize());
    }
    hashSlotsBlock = memoryManager.allocateBlock(DEFAULT_MEMORY_BLOCK_SIZE, true);
    hashSlots = (HashSlot*)hashSlotsBlock->data;
    groupHashes = make_unique<ValueVector>(nullptr, INT64, true);
    groupHashes->state = DataChunkState::getSingleValueDataChunkState();
    capacity = hashSlotsBlock->size / sizeof(HashSlot);
    bitmask = capacity - 1;
}

void AggregateHashTable::append(const vector<shared_ptr<ValueVector>>& groupVectors,
    const vector<shared_ptr<ValueVector>>& aggregateVectors) {
    if (numGroups >= capacity - 1 ||
        (double)numGroups > (double)capacity / DEFAULT_HT_LOAD_FACTOR) {
        resize(capacity * 2);
    }
    computeHashesForVectors(groupVectors);
    auto entry = findGroup(groupVectors);
    if (entry == nullptr) {
        entry = createNewGroup(groupVectors);
    }
    auto aggregateOffset = sizeof(hash_t) + numBytesForGroupKeys;
    for (auto i = 0u; i < aggregates.size(); i++) {
        aggregates[i]->update(entry + aggregateOffset, aggregateVectors[i].get(), 1);
        aggregateOffset += initializedStates[i]->getValSize();
    }
}

uint8_t* AggregateHashTable::lookup(const vector<shared_ptr<ValueVector>>& groupVectors) {
    computeHashesForVectors(groupVectors);
    return findGroup(groupVectors);
}

uint8_t* AggregateHashTable::findGroup(const vector<shared_ptr<ValueVector>>& groupVectors) {
    assert(hashSlots);
    // todo: remove the assumption that input groups are flat.
    auto hash = ((hash_t*)groupHashes->values)[0];
    auto slotOffset = hash & bitmask;
    uint8_t* entry;
    while (true) {
        auto slot = hashSlots[slotOffset];
        if (slot.blockId == EMPTY_BLOCK_ID) {
            break;
        } else if (slot.hashPrefix == hash >> HASH_PREFIX_SHIFT) {
            entry = blocks[slot.blockId - 1]->data + (numBytesPerEntry * slot.offsetInBlock);
            if (*(hash_t*)entry == hash && matchGroups(groupVectors, entry)) {
                return entry;
            }
        }
        slotOffset++;
    }
    return nullptr;
}

void AggregateHashTable::computeHashesForVectors(
    const vector<shared_ptr<ValueVector>>& vectors) const {
    VectorHashOperations::Hash(*vectors[0], *groupHashes);
    for (auto i = 1; i < vectors.size(); i++) {
        VectorHashOperations::CombineHash(*vectors[i], *groupHashes);
    }
}

bool AggregateHashTable::matchGroups(
    const vector<shared_ptr<ValueVector>>& groupVectors, uint8_t* entry) {
    entry += sizeof(hash_t); // Skip the hash value.
    for (auto& groupVector : groupVectors) {
        auto groupValueOffset = groupVector->state->getPositionOfCurrIdx();
        auto groupValue =
            groupVector->values + groupValueOffset * groupVector->getNumBytesPerValue();
        if (memcmp(entry, groupValue, groupVector->getNumBytesPerValue()) == 0) {
            entry += groupVector->getNumBytesPerValue();
        } else {
            return false;
        }
    }
    return true;
}

uint8_t* AggregateHashTable::createNewGroup(const vector<shared_ptr<ValueVector>>& groupVectors) {
    if (blocks.empty() || offsetInCurrentBlock == numEntriesPerBlock) {
        addNewBlock();
    }
    auto entry = blocks.back()->data + (offsetInCurrentBlock * numBytesPerEntry);
    auto hash = ((hash_t*)groupHashes->values)[0];
    updateHashSlot(hash, blocks.size(), offsetInCurrentBlock++);
    *(hash_t*)entry = hash;
    uint64_t offsetInEntry = sizeof(hash_t); // Skip the hash.
    for (auto& groupVector : groupVectors) {
        auto groupValueOffset = groupVector->state->getPositionOfCurrIdx();
        auto groupValue =
            groupVector->values + groupValueOffset * groupVector->getNumBytesPerValue();
        memcpy(entry + offsetInEntry, groupValue, groupVector->getNumBytesPerValue());
        offsetInEntry += groupVector->getNumBytesPerValue();
    }
    for (auto i = 0u; i < aggregates.size(); i++) {
        memcpy(entry + offsetInEntry, (uint8_t*)initializedStates[i].get(),
            initializedStates[i]->getValSize());
        offsetInEntry += initializedStates[i]->getValSize();
    }
    numGroups++;
    return entry;
}

void AggregateHashTable::addNewBlock() {
    blocks.push_back(memoryManager.allocateBlock(DEFAULT_MEMORY_BLOCK_SIZE, true));
    offsetInCurrentBlock = 0;
}

void AggregateHashTable::resize(uint64_t size) {
    assert(size > capacity);
    auto numBytesForSlots = size * sizeof(HashSlot);
    assert(numBytesForSlots > hashSlotsBlock->size);
    hashSlotsBlock = memoryManager.allocateBlock(numBytesForSlots, true);
    hashSlots = (HashSlot*)hashSlotsBlock->data;
    capacity = size;
    bitmask = capacity - 1;
    auto numEntriesToReHash = (blocks.size() - 1) * numEntriesPerBlock + offsetInCurrentBlock;
    for (auto blockId = 1u; blockId <= blocks.size(); blockId++) {
        auto numEntriesInBlock = min(numEntriesToReHash, numEntriesPerBlock);
        for (auto entryId = 0u; entryId < numEntriesInBlock; entryId++) {
            auto entry = blocks[blockId - 1]->data + (entryId * numBytesPerEntry);
            auto hash = *(hash_t*)entry;
            assert((hash & bitmask) == (hash % capacity));
            updateHashSlot(hash, blockId, entryId);
        }
        numEntriesToReHash -= numEntriesInBlock;
    }
}

void AggregateHashTable::updateHashSlot(
    hash_t hash, uint32_t blockId, uint16_t offsetInBlock) const {
    auto slotOffset = hash & bitmask;
    while (hashSlots[slotOffset].blockId != EMPTY_BLOCK_ID) {
        slotOffset++;
        if (slotOffset > capacity) {
            slotOffset = 0;
        }
    }
    assert(hashSlots[slotOffset].blockId == EMPTY_BLOCK_ID);
    hashSlots[slotOffset].hashPrefix = hash >> HASH_PREFIX_SHIFT;
    hashSlots[slotOffset].blockId = blockId;
    hashSlots[slotOffset].offsetInBlock = offsetInBlock;
}

} // namespace processor
} // namespace graphflow
