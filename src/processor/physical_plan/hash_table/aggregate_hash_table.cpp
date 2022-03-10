#include "src/processor/include/physical_plan/hash_table/aggregate_hash_table.h"

#include "src/common/include/operations/hash_operations.h"
#include "src/common/include/utils.h"

using namespace graphflow::common::operation;

namespace graphflow {
namespace processor {

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
        hasStrCol = hasStrCol || dataType == STRING;
    }
    for (auto& aggregateFunction : aggregateFunctions) {
        this->aggregateFunctions.push_back(aggregateFunction->clone());
        tableSchema.appendColumn(
            {isUnflat, dataChunkPos, aggregateFunction->getAggregateStateSize()});
    }
    factorizedTable = make_unique<FactorizedTable>(&memoryManager, tableSchema);

    maxNumHashSlots = HashTableUtils::nextPowerOfTwo(
        max(LARGE_PAGE_SIZE / sizeof(HashSlot), numEntriesToAllocate));
    bitMask = maxNumHashSlots - 1;
    numHashSlotsPerBlock = LARGE_PAGE_SIZE / sizeof(HashSlot);
    auto numDataBlocks =
        maxNumHashSlots / numHashSlotsPerBlock + (maxNumHashSlots % numHashSlotsPerBlock != 0);
    for (auto i = 0u; i < numDataBlocks; i++) {
        hashSlotsBlocks.emplace_back(make_unique<DataBlock>(&memoryManager));
    }
    distinctHashTables = AggregateHashTableUtils::createDistinctHashTables(
        memoryManager, this->groupByKeysDataTypes, this->aggregateFunctions);
}

void AggregateHashTable::append(const vector<ValueVector*>& groupByKeyVectors,
    const vector<ValueVector*>& aggregateVectors, uint64_t multiplicity) {
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
        auto hash = computeHash(inputEntry);
        auto entry = findEntry(inputEntry, hash);
        if (entry == nullptr) {
            entry = createEntry(groupByKeys, hash);
        }
        auto aggregateOffset = getAggregateStatesOffsetInEntry();
        for (auto& aggregateFunction : aggregateFunctions) {
            aggregateFunction->combineState(entry + aggregateOffset, inputEntry + aggregateOffset);
            aggregateOffset += aggregateFunction->getAggregateStateSize();
        }
    }
    factorizedTable->getOverflowBuffer()->merge(*other.factorizedTable->getOverflowBuffer());
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
        increaseSlotOffset(slotOffset);
    }
}

uint8_t* AggregateHashTable::findEntry(uint8_t* entryBuffer, hash_t hash) {
    auto slotIdx = hash & bitMask;
    while (true) {
        auto slot = getHashSlot(slotIdx);
        if (slot->groupByKeysPtr == nullptr) {
            return nullptr;
        } else if (slot->hashPrefix == hash >> HASH_PREFIX_SHIFT) {
            if (*(hash_t*)slot->groupByKeysPtr == hash &&
                matchGroupByKeys(entryBuffer, slot->groupByKeysPtr)) {
                return slot->groupByKeysPtr;
            }
        }
        increaseSlotOffset(slotIdx);
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
    fillTupleWithInitialNullAggregateState();
    fillHashSlot(hash, groupByKeysAndAggregateStateBuffer);
    return groupByKeysAndAggregateStateBuffer;
}

uint8_t* AggregateHashTable::createEntry(uint8_t* groupByKeys, hash_t hash) {
    auto groupByKeysAndAggregateStateBuffer = factorizedTable->appendEmptyTuple();
    auto nullMapOffset = factorizedTable->getTableSchema().getNullMapOffset();
    factorizedTable->updateFlatCell(
        factorizedTable->getNumTuples() - 1, 0 /* colIdx */, (void*)&hash);
    fillTupleWithGroupByKeys(
        groupByKeysAndAggregateStateBuffer + getGroupByKeysOffsetInEntry(), groupByKeys);
    fillTupleWithInitialNullAggregateState();
    fillTupleWithNullMap(groupByKeysAndAggregateStateBuffer + nullMapOffset,
        groupByKeys - getGroupByKeysOffsetInEntry() + nullMapOffset);
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
    if (slotOffset >= maxNumHashSlots) {
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
    hash_t hash;
    HashOnBytes::operation(keyVector->dataType,
        keyVector->values + pos * keyVector->getNumBytesPerValue(), keyVector->isNull(pos), hash);
    return hash;
}

hash_t AggregateHashTable::computeHash(uint8_t* keys) {
    hash_t hash = AggregateHashTable::computeHash(groupByKeysDataTypes[0], keys, 1 /* colIdx */);
    for (auto i = 1u; i < groupByKeysDataTypes.size(); ++i) {
        combineHashScalar(
            AggregateHashTable::computeHash(groupByKeysDataTypes[i], keys, 1 + i), hash);
    }
    return hash;
}

hash_t AggregateHashTable::computeHash(DataType keyDataType, uint8_t* keyValue, uint64_t colIdx) {
    hash_t hash;
    HashOnBytes::operation(keyDataType,
        keyValue + factorizedTable->getTableSchema().getColOffset(colIdx),
        FactorizedTable::isNull(
            keyValue + factorizedTable->getTableSchema().getNullMapOffset(), colIdx),
        hash);
    return hash;
}

bool AggregateHashTable::matchGroupByKeys(const vector<ValueVector*>& keyVectors, uint8_t* entry) {
    auto tableSchema = factorizedTable->getTableSchema();
    for (auto i = 0u; i < keyVectors.size(); i++) {
        auto keyVector = keyVectors[i];
        assert(keyVector->state->isFlat());
        auto pos = keyVector->state->getPositionOfCurrIdx();
        auto keyValue = keyVector->values + pos * keyVector->getNumBytesPerValue();
        auto isKeyVectorNull = keyVector->isNull(pos);
        auto isEntryKeyNull =
            FactorizedTable::isNull(entry + tableSchema.getNullMapOffset(), i + 1);
        // If either key or entry is null, we shouldn't compare the value of keyVector and entry.
        if (isKeyVectorNull && isEntryKeyNull) {
            continue;
        } else if (isKeyVectorNull != isEntryKeyNull) {
            return false;
        }
        if (!compareEntryWithKeys(keyValue, entry + tableSchema.getColOffset(i + 1),
                keyVector->dataType == STRING, keyVector->getNumBytesPerValue())) {
            return false;
        }
    }
    return true;
}

bool AggregateHashTable::matchGroupByKeys(uint8_t* entryBuffer, uint8_t* entryBufferToMatch) {
    auto tableSchema = factorizedTable->getTableSchema();
    auto nullMapOffset = tableSchema.getNullMapOffset();
    if (memcmp(entryBuffer + nullMapOffset, entryBufferToMatch + nullMapOffset,
            tableSchema.getNumBytesForNullMap())) {
        return false;
    }
    if (hasStrCol) {
        for (auto i = 0u; i < groupByKeysDataTypes.size(); i++) {
            auto colOffset = tableSchema.getColOffset(i + 1);
            if (!compareEntryWithKeys(entryBuffer + colOffset, entryBufferToMatch + colOffset,
                    groupByKeysDataTypes[i] == STRING,
                    tableSchema.getColumn(i + 1).getNumBytes())) {
                return false;
            }
        }
        return true;
    } else {
        return memcmp(entryBufferToMatch + getGroupByKeysOffsetInEntry(), entryBuffer,
                   getNumBytesForGroupByKeys()) == 0;
    }
}

void AggregateHashTable::fillTupleWithInitialNullAggregateState() {
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
        uint8_t* tuple = tupleBlock->getData();
        for (auto i = 0u; i < tupleBlock->numTuples; i++) {
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
        hashSlotsBlocks.emplace_back(make_unique<DataBlock>(&memoryManager));
    }
}

bool AggregateHashTable::compareEntryWithKeys(
    uint8_t* keyBuffer, uint8_t* tupleBuffer, bool isStrCol, uint64_t numBytesToCompare) {
    if (isStrCol) {
        return *((gf_string_t*)keyBuffer) == *((gf_string_t*)(tupleBuffer));
    } else {
        return !memcmp(keyBuffer, tupleBuffer, numBytesToCompare);
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
