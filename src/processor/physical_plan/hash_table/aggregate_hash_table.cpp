#include "src/processor/include/physical_plan/hash_table/aggregate_hash_table.h"

#include "src/common/include/operations/hash_operations.h"
#include "src/common/include/utils.h"

using namespace graphflow::common::operation;

namespace graphflow {
namespace processor {

// NOTE: We skip a tuple if it contains at least one NULL group by key which we don't support in
// current aggregate hash table. This for loop should be removed once we migrate to factorized
// table which can store NULL payloads.
static bool isKeyVectorsContainNull(const vector<ValueVector*>& keyVectors) {
    return any_of(keyVectors.begin(), keyVectors.end(), [](ValueVector* keyVector) {
        return keyVector->isNull(keyVector->state->getPositionOfCurrIdx());
    });
}

AggregateHashTable::AggregateHashTable(MemoryManager& memoryManager,
    vector<DataType> groupByKeysDataTypes,
    const vector<unique_ptr<AggregateFunction>>& aggregateFunctions, uint64_t numEntriesToAllocate)
    : BaseHashTable{memoryManager}, groupByKeysDataTypes{move(groupByKeysDataTypes)} {
    TableSchema tableSchema;
    auto isUnflat = false;
    auto dataChunkPos = 0u;
    tableSchema.appendColumn({isUnflat, dataChunkPos, sizeof(hash_t)});
    for (auto& dataType : this->groupByKeysDataTypes) {
        tableSchema.appendColumn({isUnflat, dataChunkPos, TypeUtils::getDataTypeSize(dataType)});
    }
    for (auto& aggregateFunction : aggregateFunctions) {
        this->aggregateFunctions.push_back(aggregateFunction->clone());
        tableSchema.appendColumn(
            {isUnflat, dataChunkPos, aggregateFunction->getAggregateStateSize()});
    }
    factorizedTable = make_unique<FactorizedTable>(&memoryManager, tableSchema);

    maxNumHashSlots = HashTableUtils::nextPowerOfTwo(
        max(DEFAULT_MEMORY_BLOCK_SIZE / sizeof(HashSlot), numEntriesToAllocate));
    bitMask = maxNumHashSlots - 1;
    numHashSlotsPerBlock = DEFAULT_MEMORY_BLOCK_SIZE / sizeof(HashSlot);
    auto numDataBlocks =
        maxNumHashSlots / numHashSlotsPerBlock + (maxNumHashSlots % numHashSlotsPerBlock != 0);
    for (auto i = 0u; i < numDataBlocks; i++) {
        hashSlotsBlocks.emplace_back(make_unique<DataBlock>(memoryManager.allocateOSBackedBlock(
            DEFAULT_MEMORY_BLOCK_SIZE, true /* initialized to 0 */)));
    }
    distinctHashTables = AggregateHashTableUtils::createDistinctHashTables(
        memoryManager, this->groupByKeysDataTypes, this->aggregateFunctions);
}

void AggregateHashTable::append(const vector<ValueVector*>& groupByKeyVectors,
    const vector<ValueVector*>& aggregateVectors, uint64_t multiplicity) {
    if (isKeyVectorsContainNull(groupByKeyVectors)) {
        return;
    }
    if (factorizedTable->getNumTuples() > maxNumHashSlots ||
        (double)factorizedTable->getNumTuples() >
            (double)maxNumHashSlots / DEFAULT_HT_LOAD_FACTOR) {
        resize(maxNumHashSlots * 2);
    }
    auto hash = computeHash(groupByKeyVectors);
    auto entry = findEntry(groupByKeyVectors, hash);
    if (entry == nullptr) {
        entry = createEntry(groupByKeyVectors, hash);
    }
    auto aggregateStateOffset = getAggregateStatesOffsetInEntry();
    for (auto i = 0u; i < aggregateFunctions.size(); i++) {
        auto aggregateFunction = aggregateFunctions[i].get();
        if (aggregateFunction->isFunctionDistinct()) {
            auto distinctHT = distinctHashTables[i].get();
            assert(distinctHT != nullptr);
            if (distinctHT->isAggregateValueDistinctForGroupByKeys(
                    groupByKeyVectors, aggregateVectors[i])) {
                aggregateFunctions[i]->updateState(entry + aggregateStateOffset, aggregateVectors[i], 1 /* Distinct aggregate should ignore multiplicity since they are known to be non-distinct. */);
            }
        } else {
            aggregateFunctions[i]->updateState(
                entry + aggregateStateOffset, aggregateVectors[i], multiplicity);
        }
        aggregateStateOffset += aggregateFunctions[i]->getAggregateStateSize();
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
    // Since all the columns in the factorizedTable are flat, we don't need to merge the overflow
    // dataBlocks.
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
    factorizedTable->getStringBuffer()->merge(*other.factorizedTable->getStringBuffer());
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
    auto slotOffset = hash & bitMask;
    while (true) {
        auto slot = (HashSlot*)getHashSlot(slotOffset);
        if (slot->groupByKeysPtr == nullptr) {
            return nullptr;
        } else if (slot->hashPrefix == hash >> HASH_PREFIX_SHIFT) {
            if (*(hash_t*)slot->groupByKeysPtr == hash &&
                matchGroupByKeys(groupByKeyVectors, slot->groupByKeysPtr)) {
                return slot->groupByKeysPtr;
            }
        }
        slotOffset++;
    }
}

uint8_t* AggregateHashTable::findEntry(uint8_t* groupByKeys, hash_t hash) {
    auto slotIdx = hash & bitMask;
    while (true) {
        auto slot = getHashSlot(slotIdx);
        if (slot->groupByKeysPtr == nullptr) {
            return nullptr;
        } else if (slot->hashPrefix == hash >> HASH_PREFIX_SHIFT) {
            if (*(hash_t*)slot->groupByKeysPtr == hash &&
                matchGroupByKeys(groupByKeys, slot->groupByKeysPtr)) {
                return slot->groupByKeysPtr;
            }
        }
        slotIdx++;
    }
}

uint8_t* AggregateHashTable::createEntry(
    const vector<ValueVector*>& groupByKeyVectors, hash_t hash) {
    auto groupByKeysAndAggregateStateBuffer = factorizedTable->appendEmptyTuple();
    factorizedTable->updateFlatCell(
        factorizedTable->getNumTuples() - 1, 0 /* colIdx */, (void*)&hash);
    for (auto i = 0u; i < groupByKeyVectors.size(); i++) {
        factorizedTable->updateFlatCell(
            factorizedTable->getNumTuples() - 1, i + 1, groupByKeyVectors[i]);
    }
    fillCellWithInitialNullAggregateState();
    fillHashSlot(hash, groupByKeysAndAggregateStateBuffer);
    return groupByKeysAndAggregateStateBuffer;
}

uint8_t* AggregateHashTable::createEntry(uint8_t* groupByKeys, hash_t hash) {
    auto groupByKeysAndAggregateStateBuffer = factorizedTable->appendEmptyTuple();
    factorizedTable->updateFlatCell(
        factorizedTable->getNumTuples() - 1, 0 /* colIdx */, (void*)&hash);
    memcpy(groupByKeysAndAggregateStateBuffer + getGroupByKeysOffsetInEntry(), groupByKeys,
        getNumBytesForGroupByKeys());
    fillCellWithInitialNullAggregateState();
    fillHashSlot(hash, groupByKeysAndAggregateStateBuffer);
    return groupByKeysAndAggregateStateBuffer;
}

uint64_t AggregateHashTable::getNumBytesForGroupByKeys() const {
    auto result = 0u;
    for (auto& dataType : groupByKeysDataTypes) {
        result += TypeUtils::getDataTypeSize(dataType);
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

void AggregateHashTable::fillCellWithInitialNullAggregateState() {
    auto aggregateStateStartColIdx = 1 + groupByKeysDataTypes.size();
    for (auto i = 0u; i < aggregateFunctions.size(); i++) {
        factorizedTable->updateFlatCell(factorizedTable->getNumTuples() - 1,
            aggregateStateStartColIdx + i,
            (void*)aggregateFunctions[i]->getInitialNullAggregateState());
    }
}

void AggregateHashTable::fillHashSlot(hash_t hash, uint8_t* groupByKeysAndAggregateStateBuffer) {
    auto slotIdx = hash & bitMask;
    while (getHashSlot(slotIdx)->groupByKeysPtr) {
        increaseSlotOffset(slotIdx);
    }
    getHashSlot(slotIdx)->hashPrefix = hash >> HASH_PREFIX_SHIFT;
    getHashSlot(slotIdx)->groupByKeysPtr = groupByKeysAndAggregateStateBuffer;
}

void AggregateHashTable::resize(uint64_t newSize) {
    maxNumHashSlots = newSize;
    bitMask = maxNumHashSlots - 1;
    addDataBlocksIfNecessary(maxNumHashSlots);
    for (auto& tupleBlock : factorizedTable->getTupleDataBlocks()) {
        uint8_t* tuple = tupleBlock.data;
        for (auto i = 0u; i < tupleBlock.numEntries; i++) {
            auto groupByKeysAndAggregateStateBuffer =
                tuple + i * factorizedTable->getTableSchema().getNumBytesPerTuple();
            auto newHash =
                computeHash(groupByKeysAndAggregateStateBuffer + getGroupByKeysOffsetInEntry());
            memcpy(groupByKeysAndAggregateStateBuffer, &newHash, sizeof(newHash));
            fillHashSlot(newHash, groupByKeysAndAggregateStateBuffer);
        }
    }
}

void AggregateHashTable::addDataBlocksIfNecessary(uint64_t maxNumHashSlots) {
    auto numHashSlotsBlocksNeeded =
        maxNumHashSlots / numHashSlotsPerBlock + (maxNumHashSlots % numHashSlotsPerBlock != 0);
    while (hashSlotsBlocks.size() < numHashSlotsBlocksNeeded) {
        hashSlotsBlocks.emplace_back(make_unique<DataBlock>(memoryManager.allocateOSBackedBlock(
            DEFAULT_MEMORY_BLOCK_SIZE, true /* initialized to 0 */)));
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
