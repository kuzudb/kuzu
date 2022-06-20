#include "src/processor/include/physical_plan/hash_table/aggregate_hash_table.h"

#include "src/common/include/utils.h"
#include "src/function/hash/operations/include/hash_operations.h"

using namespace graphflow::function::operation;

namespace graphflow {
namespace processor {

AggregateHashTable::AggregateHashTable(MemoryManager& memoryManager,
    vector<DataType> groupByHashKeysDataTypes, vector<DataType> groupByNonHashKeysDataTypes,
    const vector<unique_ptr<AggregateFunction>>& aggregateFunctions, uint64_t numEntriesToAllocate)
    : BaseHashTable{memoryManager}, groupByHashKeysDataTypes{move(groupByHashKeysDataTypes)},
      groupByNonHashKeysDataTypes{move(groupByNonHashKeysDataTypes)} {
    auto isUnflat = false;
    auto dataChunkPos = 0u;
    TableSchema tableSchema;
    for (auto& dataType : this->groupByHashKeysDataTypes) {
        tableSchema.appendColumn({isUnflat, dataChunkPos, Types::getDataTypeSize(dataType)});
        hasStrCol = hasStrCol || dataType.typeID == STRING;
    }
    for (auto& dataType : this->groupByNonHashKeysDataTypes) {
        tableSchema.appendColumn({isUnflat, dataChunkPos, Types::getDataTypeSize(dataType)});
        hasStrCol = hasStrCol || dataType.typeID == STRING;
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
        memoryManager, this->groupByHashKeysDataTypes, this->aggregateFunctions);
    hashValues = make_unique<hash_t[]>(DEFAULT_VECTOR_CAPACITY);
    tmpHashValues = make_unique<hash_t[]>(DEFAULT_VECTOR_CAPACITY);
    hashSlots = make_unique<HashSlot*[]>(DEFAULT_VECTOR_CAPACITY);
    aggStateColOffsetInFT = getAggregateStatesOffsetInFT();
    aggStateColIdxInFT =
        this->groupByHashKeysDataTypes.size() + this->groupByNonHashKeysDataTypes.size();
}

void AggregateHashTable::append(const vector<ValueVector*>& groupByFlatHashKeyVectors,
    const vector<ValueVector*>& groupByUnFlatHashKeyVectors,
    const vector<ValueVector*>& groupByNonHashKeyVectors,
    const vector<ValueVector*>& aggregateVectors, uint64_t multiplicity) {
    resizeHashTableIfNecessary();
    computeVectorHashes(groupByFlatHashKeyVectors, groupByUnFlatHashKeyVectors);
    findHashSlots(groupByFlatHashKeyVectors, groupByUnFlatHashKeyVectors, groupByNonHashKeyVectors);
    updateAggStates(
        groupByFlatHashKeyVectors, groupByUnFlatHashKeyVectors, aggregateVectors, multiplicity);
}

bool AggregateHashTable::isAggregateValueDistinctForGroupByKeys(
    const vector<ValueVector*>& groupByFlatKeyVectors, ValueVector* aggregateVector) {
    vector<ValueVector*> distinctKeyVectors;
    for (auto groupByFlatKeyVector : groupByFlatKeyVectors) {
        distinctKeyVectors.push_back(groupByFlatKeyVector);
    }
    distinctKeyVectors.push_back(aggregateVector);
    hash_t hash = combineHashScalar(computeFlatVecHash(groupByFlatKeyVectors),
        computeHash(aggregateVector, aggregateVector->state->getPositionOfCurrIdx()));
    auto distinctHTEntry = findEntry(distinctKeyVectors, hash);
    if (distinctHTEntry == nullptr) {
        createEntry(distinctKeyVectors, hash);
        return true;
    }
    return false;
}

void AggregateHashTable::merge(AggregateHashTable& other) {
    // Since all the columns in the factorizedTable are flat, we don't need to merge the
    // overflow dataBlocks.
    for (auto i = 0u; i < other.getNumEntries(); ++i) {
        auto inputEntry = other.getEntry(i);
        auto groupByKeys = inputEntry;
        auto hash = computeHash(inputEntry);
        auto entry = findEntry(inputEntry, hash);
        if (entry == nullptr) {
            entry = createEntry(groupByKeys, hash);
        }
        auto aggregateOffset = getAggregateStatesOffsetInFT();
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
        auto aggregateStatesOffset = getAggregateStatesOffsetInFT();
        for (auto& aggregateFunction : aggregateFunctions) {
            aggregateFunction->finalizeState(entry + aggregateStatesOffset);
            aggregateStatesOffset += aggregateFunction->getAggregateStateSize();
        }
    }
}

uint8_t* AggregateHashTable::findEntry(uint8_t* entryBuffer, hash_t hash) {
    auto slotIdx = hash & bitMask;
    while (true) {
        auto slot = getHashSlot(slotIdx);
        if (slot->entry == nullptr) {
            return nullptr;
        } else if ((slot->hash == hash) && matchGroupByKeys(entryBuffer, slot->entry)) {
            return slot->entry;
        }
        increaseSlotOffset(slotIdx);
    }
}

uint8_t* AggregateHashTable::findEntry(const vector<ValueVector*>& groupByKeyVectors, hash_t hash) {
    auto slotOffset = hash & bitMask;
    while (true) {
        auto slot = (HashSlot*)getHashSlot(slotOffset);
        if (slot->entry == nullptr) {
            return nullptr;
        } else if ((slot->hash == hash) && matchFlatGroupByKeys(groupByKeyVectors, slot->entry)) {
            return slot->entry;
        }
        increaseSlotOffset(slotOffset);
    }
}

uint8_t* AggregateHashTable::createEntry(const vector<ValueVector*>& groupByFlatHashKeyVectors,
    const vector<ValueVector*>& groupByUnflatHashKeyVectors,
    const vector<ValueVector*>& groupByNonHashKeyVectors, hash_t hash, uint32_t unflatVectorIdx,
    HashSlot* slot) {
    auto entry = factorizedTable->appendEmptyTuple();
    // The first column in factorizedTable stores the hashValue, we should store group by keys
    // starting from the second column in factorizedTable.
    auto colIdx = 0u;
    for (auto flatKeyVector : groupByFlatHashKeyVectors) {
        factorizedTable->updateFlatCell(
            entry, colIdx, flatKeyVector, flatKeyVector->state->getPositionOfCurrIdx());
        colIdx++;
    }
    for (auto unflatKeyVector : groupByUnflatHashKeyVectors) {
        factorizedTable->updateFlatCell(entry, colIdx, unflatKeyVector, unflatVectorIdx);
        colIdx++;
    }
    for (auto nonHashKeyVector : groupByNonHashKeyVectors) {
        factorizedTable->updateFlatCell(entry, colIdx, nonHashKeyVector,
            nonHashKeyVector->state->isFlat() ? nonHashKeyVector->state->getPositionOfCurrIdx() :
                                                unflatVectorIdx);
        colIdx++;
    }
    fillTupleWithInitialNullAggregateState(entry);
    slot->hash = hash;
    slot->entry = entry;
    return entry;
}

uint8_t* AggregateHashTable::createEntry(uint8_t* groupByKeys, hash_t hash) {
    auto groupByKeysAndAggregateStateBuffer = factorizedTable->appendEmptyTuple();
    auto nullMapOffset = factorizedTable->getTableSchema().getNullMapOffset();
    fillTupleWithGroupByKeys(groupByKeysAndAggregateStateBuffer, groupByKeys);
    fillTupleWithInitialNullAggregateState(groupByKeysAndAggregateStateBuffer);
    fillTupleWithNullMap(
        groupByKeysAndAggregateStateBuffer + nullMapOffset, groupByKeys + nullMapOffset);
    fillHashSlot(hash, groupByKeysAndAggregateStateBuffer);
    return groupByKeysAndAggregateStateBuffer;
}

uint8_t* AggregateHashTable::createEntry(
    const vector<ValueVector*>& groupByHashKeyVectors, hash_t hash) {
    auto entry = factorizedTable->appendEmptyTuple();
    for (auto i = 0u; i < groupByHashKeyVectors.size(); i++) {
        factorizedTable->updateFlatCell(entry, i, groupByHashKeyVectors[i],
            groupByHashKeyVectors[i]->state->getPositionOfCurrIdx());
    }
    fillTupleWithInitialNullAggregateState(entry);
    fillHashSlot(hash, entry);
    return entry;
}

uint64_t AggregateHashTable::getNumBytesForGroupByHashKeys() const {
    auto result = 0u;
    for (auto& dataType : groupByHashKeysDataTypes) {
        result += Types::getDataTypeSize(dataType);
    }
    return result;
}

uint64_t AggregateHashTable::getNumBytesForGroupByNonHashKeys() const {
    auto result = 0u;
    for (auto& dataType : groupByNonHashKeysDataTypes) {
        result += Types::getDataTypeSize(dataType);
    }
    return result;
}

void AggregateHashTable::increaseSlotOffset(uint64_t& slotOffset) const {
    slotOffset++;
    if (slotOffset >= maxNumHashSlots) {
        slotOffset = 0;
    }
}

void AggregateHashTable::findHashSlots(const vector<ValueVector*>& groupByFlatHashKeyVectors,
    const vector<ValueVector*>& groupByUnFlatHashKeyVectors,
    const vector<ValueVector*>& groupByNonHashKeyVectors) {
    auto size = groupByUnFlatHashKeyVectors.empty() ?
                    1 :
                    groupByUnFlatHashKeyVectors[0]->state->selectedSize;
    for (auto i = 0u; i < size; i++) {
        auto hash = hashValues[i];
        auto slotOffset = hash & bitMask;
        auto selectedPos = groupByUnFlatHashKeyVectors.empty() ?
                               0 :
                               groupByUnFlatHashKeyVectors[0]->state->selectedPositions[i];
        while (true) {
            auto slot = getHashSlot(slotOffset);
            if (slot->entry == nullptr) {
                createEntry(groupByFlatHashKeyVectors, groupByUnFlatHashKeyVectors,
                    groupByNonHashKeyVectors, hash, selectedPos, slot);
                hashSlots[i] = slot;
                break;
            } else if ((slot->hash == hash) &&
                       matchGroupByKeys(groupByFlatHashKeyVectors, groupByUnFlatHashKeyVectors,
                           slot->entry, selectedPos)) {
                hashSlots[i] = slot;
                break;
            }
            increaseSlotOffset(slotOffset);
        }
    }
}

hash_t AggregateHashTable::computeHash(ValueVector* keyVector, uint32_t pos) {
    hash_t hash;
    HashOnBytes::operation(keyVector->dataType.typeID,
        keyVector->values + pos * keyVector->getNumBytesPerValue(), keyVector->isNull(pos), hash);
    return hash;
}

hash_t AggregateHashTable::computeFlatVecHash(
    const vector<ValueVector*>& groupByFlatHashKeyVectors) {
    if (groupByFlatHashKeyVectors.empty()) {
        return 0;
    }
    hash_t hash = computeHash(
        groupByFlatHashKeyVectors[0], groupByFlatHashKeyVectors[0]->state->getPositionOfCurrIdx());
    for (auto i = 1u; i < groupByFlatHashKeyVectors.size(); i++) {
        hash = combineHashScalar(
            hash, computeHash(groupByFlatHashKeyVectors[i],
                      groupByFlatHashKeyVectors[i]->state->getPositionOfCurrIdx()));
    }
    return hash;
}

void AggregateHashTable::computeUnflatVecHash(
    const vector<ValueVector*>& groupByUnflatHashKeyVectors, uint32_t startVecIdx,
    hash_t initHashVal) {
    for (; startVecIdx < groupByUnflatHashKeyVectors.size(); startVecIdx++) {
        for (auto i = 0u; i < groupByUnflatHashKeyVectors[0]->state->selectedSize; i++) {
            tmpHashValues[i] = computeHash(groupByUnflatHashKeyVectors[startVecIdx],
                groupByUnflatHashKeyVectors[0]->state->selectedPositions[i]);
        }
        for (auto i = 0u; i < groupByUnflatHashKeyVectors[0]->state->selectedSize; i++) {
            hashValues[i] =
                combineHashScalar(startVecIdx == 0 ? initHashVal : hashValues[i], tmpHashValues[i]);
        }
    }
}

void AggregateHashTable::computeVectorHashes(const vector<ValueVector*>& groupByFlatHashKeyVectors,
    const vector<ValueVector*>& groupByUnflatHashKeyVectors) {
    if (!groupByFlatHashKeyVectors.empty()) {
        auto hash = computeFlatVecHash(groupByFlatHashKeyVectors);
        hashValues[0] = hash;
        computeUnflatVecHash(groupByUnflatHashKeyVectors, 0, hash);
    } else {
        auto unflatVector = groupByUnflatHashKeyVectors[0];
        for (auto i = 0u; i < unflatVector->state->selectedSize; i++) {
            hashValues[i] = computeHash(unflatVector, unflatVector->state->selectedPositions[i]);
        }
        computeUnflatVecHash(groupByUnflatHashKeyVectors, 1, 0 /* dummy initHashVal */);
    }
}

hash_t AggregateHashTable::computeHash(uint8_t* keys) {
    hash_t hash = computeHash(groupByHashKeysDataTypes[0], keys, 0 /* colIdx */);
    for (auto i = 1u; i < groupByHashKeysDataTypes.size(); ++i) {
        combineHashScalar(
            AggregateHashTable::computeHash(groupByHashKeysDataTypes[i], keys, i), hash);
    }
    return hash;
}

hash_t AggregateHashTable::computeHash(
    const DataType& keyDataType, uint8_t* keyValue, uint64_t colIdx) {
    assert(keyDataType.typeID != LIST);
    hash_t hash;
    HashOnBytes::operation(keyDataType.typeID,
        keyValue + factorizedTable->getTableSchema().getColOffset(colIdx),
        FactorizedTable::isNull(
            keyValue + factorizedTable->getTableSchema().getNullMapOffset(), colIdx),
        hash);
    return hash;
}

void AggregateHashTable::updateAggStates(const vector<ValueVector*>& groupByFlatHashKeyVectors,
    const vector<ValueVector*>& groupByUnFlatHashKeyVectors,
    const vector<ValueVector*>& aggregateVectors, uint64_t multiplicity) {
    auto size = groupByUnFlatHashKeyVectors.empty() ?
                    1 :
                    groupByUnFlatHashKeyVectors[0]->state->selectedSize;
    for (auto i = 0u; i < size; i++) {
        auto aggregateStateOffset = aggStateColOffsetInFT;
        for (auto j = 0u; j < aggregateFunctions.size(); j++) {
            auto aggregateFunction = aggregateFunctions[j].get();
            auto aggVector = aggregateVectors[j];
            if (aggregateFunction->isFunctionDistinct()) {
                auto distinctHT = distinctHashTables[j].get();
                assert(distinctHT != nullptr);
                if (distinctHT->isAggregateValueDistinctForGroupByKeys(
                        groupByFlatHashKeyVectors, aggregateVectors[j])) {
                    aggregateFunctions[j]->updatePosState(
                        hashSlots[i]->entry + aggregateStateOffset, aggregateVectors[j],
                        1 /* Distinct aggregate should ignore multiplicity
                             since they are known to be non-distinct. */
                        ,
                        aggVector->state->getPositionOfCurrIdx());
                }
            } else {
                if (aggVector &&
                    (aggVector->state->isFlat() ||
                        (!groupByUnFlatHashKeyVectors.empty() &&
                            aggVector->state == groupByUnFlatHashKeyVectors[0]->state))) {
                    aggregateFunctions[j]->updatePosState(
                        hashSlots[i]->entry + aggregateStateOffset, aggregateVectors[j],
                        multiplicity,
                        aggVector->state->isFlat() ? aggVector->state->getPositionOfCurrIdx() :
                                                     aggVector->state->selectedPositions[i]);
                } else {
                    aggregateFunctions[j]->updateAllState(
                        hashSlots[i]->entry + aggregateStateOffset, aggregateVectors[j],
                        multiplicity);
                }
            }
            aggregateStateOffset += aggregateFunctions[j]->getAggregateStateSize();
        }
    }
}

bool AggregateHashTable::matchFlatGroupByKeys(
    const vector<ValueVector*>& keyVectors, uint8_t* entry) {
    for (auto i = 0u; i < keyVectors.size(); i++) {
        auto keyVector = keyVectors[i];
        assert(keyVector->state->isFlat());
        auto pos = keyVector->state->getPositionOfCurrIdx();
        auto keyValue = keyVector->values + pos * keyVector->getNumBytesPerValue();
        auto isKeyVectorNull = keyVector->isNull(pos);
        auto isEntryKeyNull = FactorizedTable::isNull(
            entry + factorizedTable->getTableSchema().getNullMapOffset(), i);
        // If either key or entry is null, we shouldn't compare the value of keyVector and
        // entry.
        if (isKeyVectorNull && isEntryKeyNull) {
            continue;
        } else if (isKeyVectorNull != isEntryKeyNull) {
            return false;
        }
        if (!compareEntryWithKeys(keyVector->dataType.typeID == STRING, keyValue,
                entry + factorizedTable->getTableSchema().getColOffset(i),
                keyVector->getNumBytesPerValue())) {
            return false;
        }
    }
    return true;
}

bool AggregateHashTable::matchGroupByKey(
    ValueVector* keyVector, uint8_t* entry, uint32_t pos, uint32_t colIdx) {
    auto keyValue = keyVector->values + pos * keyVector->getNumBytesPerValue();
    auto isKeyVectorNull = keyVector->isNull(pos);
    auto isEntryKeyNull = FactorizedTable::isNull(
        entry + factorizedTable->getTableSchema().getNullMapOffset(), colIdx);
    // If either key or entry is null, we shouldn't compare the value of keyVector and entry.
    if (isKeyVectorNull && isEntryKeyNull) {
        return true;
    } else if (isKeyVectorNull != isEntryKeyNull) {
        return false;
    }
    return compareEntryWithKeys(keyVector->dataType.typeID == STRING, keyValue,
        entry + factorizedTable->getTableSchema().getColOffset(colIdx),
        keyVector->getNumBytesPerValue());
}

bool AggregateHashTable::matchGroupByKeys(const vector<ValueVector*>& groupByFlatHashKeyVectors,
    const vector<ValueVector*>& groupByUnflatHashKeyVectors, uint8_t* entry,
    uint32_t unflatVectorIdx) {
    auto colIdx = 0u;
    for (auto& keyVector : groupByFlatHashKeyVectors) {
        if (!matchGroupByKey(keyVector, entry, keyVector->state->getPositionOfCurrIdx(), colIdx)) {
            return false;
        }
        colIdx++;
    }
    for (auto& keyVector : groupByUnflatHashKeyVectors) {
        if (!matchGroupByKey(keyVector, entry, unflatVectorIdx, colIdx)) {
            return false;
        }
        colIdx++;
    }
    return true;
}

bool AggregateHashTable::matchGroupByKeys(uint8_t* entryBuffer, uint8_t* entryBufferToMatch) {
    auto nullMapOffset = factorizedTable->getTableSchema().getNullMapOffset();
    if (memcmp(entryBuffer + nullMapOffset, entryBufferToMatch + nullMapOffset,
            factorizedTable->getTableSchema().getNumBytesForNullMap())) {
        return false;
    }
    if (hasStrCol) {
        for (auto i = 0u; i < groupByHashKeysDataTypes.size(); i++) {
            auto colOffset = factorizedTable->getTableSchema().getColOffset(i);
            if (!compareEntryWithKeys(groupByHashKeysDataTypes[i].typeID == STRING,
                    entryBuffer + colOffset, entryBufferToMatch + colOffset,
                    factorizedTable->getTableSchema().getColumn(i).getNumBytes())) {
                return false;
            }
        }
        return true;
    } else {
        return memcmp(entryBufferToMatch, entryBuffer, getNumBytesForGroupByHashKeys()) == 0;
    }
}

void AggregateHashTable::fillTupleWithInitialNullAggregateState(uint8_t* tuple) {
    for (auto i = 0u; i < aggregateFunctions.size(); i++) {
        factorizedTable->updateFlatCell(tuple, aggStateColIdxInFT + i,
            (void*)aggregateFunctions[i]->getInitialNullAggregateState());
    }
}

void AggregateHashTable::fillHashSlot(hash_t hash, uint8_t* groupByKeysAndAggregateStateBuffer) {
    auto slotIdx = hash & bitMask;
    while (getHashSlot(slotIdx)->entry) {
        increaseSlotOffset(slotIdx);
    }
    auto slot = getHashSlot(slotIdx);
    slot->hash = hash;
    slot->entry = groupByKeysAndAggregateStateBuffer;
}

void AggregateHashTable::resize(uint64_t newSize) {
    maxNumHashSlots = newSize;
    bitMask = maxNumHashSlots - 1;
    addDataBlocksIfNecessary(maxNumHashSlots);
    for (auto& block : hashSlotsBlocks) {
        block->resetToZero();
    }
    for (auto& tupleBlock : factorizedTable->getTupleDataBlocks()) {
        uint8_t* tuple = tupleBlock->getData();
        for (auto i = 0u; i < tupleBlock->numTuples; i++) {
            auto groupByKeysAndAggregateStateBuffer =
                tuple + i * factorizedTable->getTableSchema().getNumBytesPerTuple();
            auto newHash = computeHash(groupByKeysAndAggregateStateBuffer);
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
    bool isStrColumn, uint8_t* keyValue, uint8_t* entry, uint32_t numBytesToCompare) {
    return isStrColumn ? *((gf_string_t*)keyValue) == *((gf_string_t*)entry) :
                         memcmp(keyValue, entry, numBytesToCompare) == 0;
}

void AggregateHashTable::resizeHashTableIfNecessary() {
    if (factorizedTable->getNumTuples() > maxNumHashSlots ||
        (double)factorizedTable->getNumTuples() >
            (double)maxNumHashSlots / DEFAULT_HT_LOAD_FACTOR) {
        resize(maxNumHashSlots * 2);
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
