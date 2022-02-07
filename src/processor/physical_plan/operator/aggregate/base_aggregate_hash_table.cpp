#include "src/processor/include/physical_plan/operator/aggregate/base_aggregate_hash_table.h"

#include "src/common/include/operations/hash_operations.h"
#include "src/common/include/utils.h"

using namespace graphflow::common::operation;

namespace graphflow {
namespace processor {

// NOTE: We skip a tuple if it contains at least one NULL group by key which we don't support in
// current aggregate hash table. This for loop should be removed once we migrate to factorized
// table which can store NULL payloads.
static bool isKeyVectorsContainNull(const vector<ValueVector*>& keyVectors) {
    for (auto& keyVector : keyVectors) {
        if (keyVector->isNull(keyVector->state->getPositionOfCurrIdx())) {
            return true;
        }
    }
    return false;
}

AggregateHashTable::AggregateHashTable(MemoryManager& memoryManager,
    vector<DataType> groupByKeysDataTypes,
    const vector<unique_ptr<AggregateFunction>>& aggregateFunctions, uint64_t numEntriesToAllocate)
    : memoryManager{memoryManager}, groupByKeysDataTypes{move(groupByKeysDataTypes)}, numEntries{
                                                                                          0} {
    for (auto& aggregateFunction : aggregateFunctions) {
        this->aggregateFunctions.push_back(aggregateFunction->clone());
    }
    maxNumHashSlots = HashTableUtils::nextPowerOfTwo(
        max(DEFAULT_MEMORY_BLOCK_SIZE / sizeof(HashSlot), numEntriesToAllocate));
    bitmask = maxNumHashSlots - 1;
    auto numHashSlotBlocks = maxNumHashSlots * sizeof(HashSlot) / DEFAULT_MEMORY_BLOCK_SIZE + 1;
    // TODO: we should use multiple memory block to hold hash slots
    hashSlotsBlock = memoryManager.allocateBlock(
        numHashSlotBlocks * DEFAULT_MEMORY_BLOCK_SIZE, true /* initialized to 0 */);
    hashSlots = (HashSlot*)hashSlotsBlock->data;
    // Block 0 is reserved as empty block because block id 0 is used to identify empty block.
    entryBlocks.push_back(memoryManager.allocateBlock(0, true /* initialized to 0 */));
    numEntriesPerBlock = DEFAULT_MEMORY_BLOCK_SIZE / getNumBytesForEntry();
    currentEntryCursor.entryBlockId = 0;
    currentEntryCursor.offsetInBlock = numEntriesPerBlock; // skip the first empty block
    distinctHashTables = AggregateHashTableUtils::createDistinctHashTables(
        memoryManager, this->groupByKeysDataTypes, this->aggregateFunctions);
}

uint8_t* AggregateHashTable::getEntry(uint64_t id) {
    auto blockId = id / numEntriesPerBlock + 1; // skip the first empty block
    auto offsetInBlock = id % numEntriesPerBlock;
    return getEntry(blockId, offsetInBlock);
}

void AggregateHashTable::append(const vector<ValueVector*>& groupByKeyVectors,
    const vector<ValueVector*>& aggregateVectors, uint64_t multiplicity) {
    if (isKeyVectorsContainNull(groupByKeyVectors)) {
        return;
    }
    if (numEntries > maxNumHashSlots ||
        (double)numEntries > (double)maxNumHashSlots / DEFAULT_HT_LOAD_FACTOR) {
        resize(maxNumHashSlots * 2);
    }
    auto hash = computeHash(groupByKeyVectors);
    auto entry = findEntry(groupByKeyVectors, hash);
    if (entry == nullptr) {
        entry = createEntry(groupByKeyVectors, hash);
    }
    auto aggregateOffset = getAggregateStatesOffsetInEntry();
    for (auto i = 0u; i < aggregateFunctions.size(); i++) {
        auto aggregateFunction = aggregateFunctions[i].get();
        if (aggregateFunction->isFunctionDistinct()) {
            auto distinctHT = distinctHashTables[i].get();
            assert(distinctHT != nullptr);
            if (distinctHT->isAggregateValueDistinctForGroupByKeys(
                    groupByKeyVectors, aggregateVectors[i])) {
                aggregateFunctions[i]->updateState(entry + aggregateOffset, aggregateVectors[i], 1 /* Distinct aggregate should ignore multiplicity since they are known to be non-distinct. */);
            }
        } else {
            aggregateFunctions[i]->updateState(
                entry + aggregateOffset, aggregateVectors[i], multiplicity);
        }
        aggregateOffset += aggregateFunctions[i]->getAggregateStateSize();
    }
}

bool AggregateHashTable::isAggregateValueDistinctForGroupByKeys(
    const vector<ValueVector*>& groupByKeyVectors, ValueVector* aggregateVector) {
    vector<ValueVector*> distinctKeyVectors;
    for (auto& groupByKeyVector : groupByKeyVectors) {
        distinctKeyVectors.push_back(groupByKeyVector);
    }
    distinctKeyVectors.push_back(aggregateVector);
    if (isKeyVectorsContainNull(distinctKeyVectors)) {
        return false;
    }
    auto distinctHTHash = computeHash(distinctKeyVectors);
    auto distinctHTEntry = findEntry(distinctKeyVectors, distinctHTHash);
    if (distinctHTEntry == nullptr) {
        createEntry(distinctKeyVectors, distinctHTHash);
        return true;
    }
    return false;
}

void AggregateHashTable::merge(AggregateHashTable& other) {
    for (auto i = 0u; i < other.getNumEntries(); ++i) {
        auto inputEntry = other.getEntry(i);
        auto groupByKeys = inputEntry + getGroupByKeysOffsetInEntry();
        auto hash = computeHash(groupByKeys);
        auto entry = findEntry(groupByKeys, hash);
        if (entry == nullptr) {
            entry = createEntry(groupByKeys, hash);
        }
        auto aggregateOffset = getAggregateStatesOffsetInEntry();
        for (auto& aggregateFunction : aggregateFunctions) {
            aggregateFunction->combineState(entry + aggregateOffset, inputEntry + aggregateOffset);
            aggregateOffset += aggregateFunction->getAggregateStateSize();
        }
    }
}

void AggregateHashTable::finalizeAggregateStates() {
    for (auto i = 0u; i < getNumEntries(); ++i) {
        auto entry = getEntry(i);
        auto aggregateStatesOffset = getAggregateStatesOffsetInEntry();
        for (auto& aggregateFunction : aggregateFunctions) {
            aggregateFunction->finalizeState(entry + aggregateStatesOffset);
            aggregateStatesOffset += aggregateFunction->getAggregateStateSize();
        }
    }
}

uint8_t* AggregateHashTable::findEntry(const vector<ValueVector*>& groupByKeyVectors, hash_t hash) {
    auto slotOffset = hash & bitmask;
    uint8_t* entry;
    while (true) {
        auto slot = hashSlots[slotOffset];
        if (slot.blockId == EMPTY_BLOCK_ID) {
            return nullptr;
        } else if (slot.hashPrefix == hash >> HASH_PREFIX_SHIFT) {
            entry = getEntry(slot.blockId, slot.offsetInBlock);
            if (*(hash_t*)entry == hash && matchGroupByKeys(groupByKeyVectors, entry)) {
                return entry;
            }
        }
        slotOffset++;
    }
}

uint8_t* AggregateHashTable::findEntry(uint8_t* groupByKeys, hash_t hash) {
    auto slotOffset = hash & bitmask;
    uint8_t* entry;
    while (true) {
        auto slot = hashSlots[slotOffset];
        if (slot.blockId == EMPTY_BLOCK_ID) {
            return nullptr;
        } else if (slot.hashPrefix == hash >> HASH_PREFIX_SHIFT) {
            entry = getEntry(slot.blockId, slot.offsetInBlock);
            if (*(hash_t*)entry == hash && matchGroupByKeys(groupByKeys, entry)) {
                return entry;
            }
        }
        slotOffset++;
    }
}

uint8_t* AggregateHashTable::createEntry(
    const vector<ValueVector*>& groupByKeyVectors, hash_t hash) {
    if (isCurrentEntryFull()) {
        addNewBlock();
    }
    auto entry = getCurrentEntry();
    fillEntryWithHash(entry, hash);
    // fill group values into entry
    uint64_t offset = getGroupByKeysOffsetInEntry();
    for (auto& keyVector : groupByKeyVectors) {
        auto pos = keyVector->state->getPositionOfCurrIdx();
        memcpy(entry + offset, keyVector->values + pos * keyVector->getNumBytesPerValue(),
            keyVector->getNumBytesPerValue());
        offset += keyVector->getNumBytesPerValue();
    }
    fillEntryWithInitialNullAggregateState(entry);
    fillHashSlot(hash, currentEntryCursor.entryBlockId, currentEntryCursor.offsetInBlock);
    currentEntryCursor.offsetInBlock++;
    numEntries++;
    return entry;
}

uint8_t* AggregateHashTable::createEntry(uint8_t* groupByKeys, hash_t hash) {
    if (isCurrentEntryFull()) {
        addNewBlock();
    }
    auto entry = getCurrentEntry();
    fillEntryWithHash(entry, hash);
    // fill group values into entry
    memcpy(entry + getGroupByKeysOffsetInEntry(), groupByKeys, getNumBytesForGroupByKeys());
    fillEntryWithInitialNullAggregateState(entry);
    fillHashSlot(hash, currentEntryCursor.entryBlockId, currentEntryCursor.offsetInBlock);
    currentEntryCursor.offsetInBlock++;
    numEntries++;
    return entry;
}

uint64_t AggregateHashTable::getNumBytesForGroupByKeys() const {
    auto result = 0u;
    for (auto& dataType : groupByKeysDataTypes) {
        result += TypeUtils::getDataTypeSize(dataType);
    }
    return result;
}

uint64_t AggregateHashTable::getNumBytesForAggregateStates() const {
    auto result = 0u;
    for (auto& aggregateFunction : aggregateFunctions) {
        result += aggregateFunction->getAggregateStateSize();
    }
    return result;
}

void AggregateHashTable::increaseSlotOffset(uint64_t& slotOffset) const {
    slotOffset++;
    if (slotOffset > maxNumHashSlots) {
        slotOffset = 0;
    }
}

hash_t AggregateHashTable::computeHash(const vector<ValueVector*>& keyVectors) {
    auto hash = computeHash(keyVectors[0]);
    for (auto i = 1u; i < keyVectors.size(); ++i) {
        hash = combineHashScalar(computeHash(keyVectors[i]), hash);
    }
    return hash;
}

hash_t AggregateHashTable::computeHash(ValueVector* keyVector) {
    assert(keyVector->state->isFlat());
    auto pos = keyVector->state->getPositionOfCurrIdx();
    return AggregateHashTable::computeHash(
        keyVector->dataType, keyVector->values + pos * keyVector->getNumBytesPerValue());
}

hash_t AggregateHashTable::computeHash(uint8_t* keys) {
    hash_t hash = AggregateHashTable::computeHash(groupByKeysDataTypes[0], keys);
    auto offset = TypeUtils::getDataTypeSize(groupByKeysDataTypes[0]);
    for (auto i = 1u; i < groupByKeysDataTypes.size(); ++i) {
        combineHashScalar(
            AggregateHashTable::computeHash(groupByKeysDataTypes[i], keys + offset), hash);
        offset += TypeUtils::getDataTypeSize(groupByKeysDataTypes[i]);
    }
    return hash;
}

hash_t AggregateHashTable::computeHash(DataType keyDataType, uint8_t* keyValue) {
    hash_t hash;
    HashOnBytes::operation(keyDataType, keyValue, false /* isNull */, hash);
    return hash;
}

bool AggregateHashTable::matchGroupByKeys(const vector<ValueVector*>& keyVectors, uint8_t* entry) {
    auto offset = getGroupByKeysOffsetInEntry();
    for (auto& keyVector : keyVectors) {
        if (!matchGroupByKey(keyVector, entry + offset)) {
            return false;
        }
        offset += keyVector->getNumBytesPerValue();
    }
    return true;
}

bool AggregateHashTable::matchGroupByKey(ValueVector* keyVector, uint8_t* value) {
    assert(keyVector->state->isFlat());
    auto pos = keyVector->state->getPositionOfCurrIdx();
    auto groupKey = keyVector->values + pos * keyVector->getNumBytesPerValue();
    return memcmp(value, groupKey, keyVector->getNumBytesPerValue()) == 0;
}

bool AggregateHashTable::matchGroupByKeys(uint8_t* keys, uint8_t* entry) {
    auto keysInEntry = entry + getGroupByKeysOffsetInEntry();
    return memcmp(keysInEntry, keys, getNumBytesForGroupByKeys()) == 0;
}

void AggregateHashTable::fillEntryWithHash(uint8_t* entry, hash_t hash) {
    *(hash_t*)entry = hash;
}

void AggregateHashTable::fillEntryWithInitialNullAggregateState(uint8_t* entry) {
    auto offset = getAggregateStatesOffsetInEntry();
    for (auto& aggregateFunction : aggregateFunctions) {
        memcpy(entry + offset, (uint8_t*)aggregateFunction->getInitialNullAggregateState(),
            aggregateFunction->getAggregateStateSize());
        offset += aggregateFunction->getAggregateStateSize();
    }
}

void AggregateHashTable::fillHashSlot(hash_t hash, uint32_t blockId, uint16_t offsetInBlock) {
    auto slotOffset = hash & bitmask;
    while (hashSlots[slotOffset].blockId != EMPTY_BLOCK_ID) {
        increaseSlotOffset(slotOffset);
    }
    hashSlots[slotOffset].hashPrefix = hash >> HASH_PREFIX_SHIFT;
    hashSlots[slotOffset].blockId = blockId;
    hashSlots[slotOffset].offsetInBlock = offsetInBlock;
}

void AggregateHashTable::addNewBlock() {
    entryBlocks.push_back(
        memoryManager.allocateBlock(DEFAULT_MEMORY_BLOCK_SIZE, true /* initialized to 0 */));
    currentEntryCursor.entryBlockId = entryBlocks.size() - 1;
    currentEntryCursor.offsetInBlock = 0;
}

void AggregateHashTable::resize(uint64_t newSize) {
    maxNumHashSlots = newSize;
    bitmask = maxNumHashSlots - 1;
    hashSlotsBlock = memoryManager.allocateBlock(
        maxNumHashSlots * sizeof(HashSlot), true /* initialized to 0 */);
    hashSlots = (HashSlot*)hashSlotsBlock->data;
    for (auto blockId = 1u; blockId < currentEntryCursor.entryBlockId; ++blockId) {
        for (auto offsetInBlock = 0u; offsetInBlock < currentEntryCursor.offsetInBlock;
             ++offsetInBlock) {
            auto entry = getEntry(blockId, offsetInBlock);
            auto newHash = computeHash(entry + getGroupByKeysOffsetInEntry());
            fillEntryWithHash(entry, newHash);
            fillHashSlot(newHash, blockId, offsetInBlock);
        }
    }
}

vector<unique_ptr<AggregateHashTable>> AggregateHashTableUtils::createDistinctHashTables(
    MemoryManager& memoryManager, const vector<DataType>& groupByKeyDataTypes,
    const vector<unique_ptr<AggregateFunction>>& aggregateFunctions) {
    vector<unique_ptr<AggregateHashTable>> distinctHTs;
    for (auto& aggregateFunction : aggregateFunctions) {
        if (aggregateFunction->isFunctionDistinct()) {
            vector<DataType> distinctKeysDataTypes;
            for (auto& groupByKeyDataType : groupByKeyDataTypes) {
                distinctKeysDataTypes.push_back(groupByKeyDataType);
            }
            distinctKeysDataTypes.push_back(aggregateFunction->getInputDataType());
            vector<unique_ptr<AggregateFunction>> emptyFunctions;
            auto ht = make_unique<AggregateHashTable>(
                memoryManager, distinctKeysDataTypes, emptyFunctions, 0 /* numEntriesToAllocate */);
            distinctHTs.push_back(move(ht));
        } else {
            distinctHTs.push_back(nullptr);
        }
    }
    return distinctHTs;
}

} // namespace processor
} // namespace graphflow
