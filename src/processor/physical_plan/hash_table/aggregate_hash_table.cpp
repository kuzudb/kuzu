#include "src/processor/include/physical_plan/hash_table/aggregate_hash_table.h"

#include "src/common/include/utils.h"
#include "src/function/aggregate/include/base_count.h"
#include "src/function/hash/include/vector_hash_operations.h"
#include "src/function/include/unary_operation_executor.h"
using namespace graphflow::function::operation;

namespace graphflow {
namespace processor {

AggregateHashTable::AggregateHashTable(MemoryManager& memoryManager,
    vector<DataType> groupByHashKeysDataTypes, vector<DataType> groupByNonHashKeysDataTypes,
    const vector<unique_ptr<AggregateFunction>>& aggregateFunctions, uint64_t numEntriesToAllocate)
    : BaseHashTable{memoryManager}, groupByHashKeysDataTypes{move(groupByHashKeysDataTypes)},
      groupByNonHashKeysDataTypes{move(groupByNonHashKeysDataTypes)} {
    initializeFT(aggregateFunctions);
    initializeHashTable(numEntriesToAllocate);
    distinctHashTables = AggregateHashTableUtils::createDistinctHashTables(
        memoryManager, this->groupByHashKeysDataTypes, this->aggregateFunctions);
    initializeTmpVectors();
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
    if (groupByFlatKeyVectors.empty()) {
        VectorHashOperations::computeHash(aggregateVector, hashVector.get());
    } else {
        VectorHashOperations::computeHash(groupByFlatKeyVectors[0], hashVector.get());
        computeAndCombineVecHash(groupByFlatKeyVectors, 1 /* startVecIdx */);
        VectorHashOperations::computeHash(aggregateVector, tmpHashVector.get());
        VectorHashOperations::combineHash(hashVector.get(), tmpHashVector.get(), hashVector.get());
    }
    hash_t hash = *(hash_t*)(hashVector->values + hashVector->getNumBytesPerValue() *
                                                      hashVector->state->getPositionOfCurrIdx());
    auto distinctHTEntry = findEntryInDistinctHT(distinctKeyVectors, hash);
    if (distinctHTEntry == nullptr) {
        createEntryInDistinctHT(distinctKeyVectors, hash);
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
        auto aggregateOffset = aggStateColOffsetInFT;
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
        auto aggregateStatesOffset = aggStateColOffsetInFT;
        for (auto& aggregateFunction : aggregateFunctions) {
            aggregateFunction->finalizeState(entry + aggregateStatesOffset);
            aggregateStatesOffset += aggregateFunction->getAggregateStateSize();
        }
    }
}

void AggregateHashTable::initializeHashTable(uint64_t numEntriesToAllocate) {
    maxNumHashSlots = HashTableUtils::nextPowerOfTwo(
        max(LARGE_PAGE_SIZE / sizeof(HashSlot), numEntriesToAllocate));
    bitMask = maxNumHashSlots - 1;
    numHashSlotsPerBlock = LARGE_PAGE_SIZE / sizeof(HashSlot);
    auto numDataBlocks =
        maxNumHashSlots / numHashSlotsPerBlock + (maxNumHashSlots % numHashSlotsPerBlock != 0);
    for (auto i = 0u; i < numDataBlocks; i++) {
        hashSlotsBlocks.emplace_back(make_unique<DataBlock>(&memoryManager));
    }
}

void AggregateHashTable::initializeFT(const vector<unique_ptr<AggregateFunction>>& aggFuncs) {
    auto isUnflat = false;
    auto dataChunkPos = 0u;
    TableSchema tableSchema;
    aggStateColIdxInFT =
        this->groupByHashKeysDataTypes.size() + this->groupByNonHashKeysDataTypes.size();
    compareFuncs.resize(aggStateColIdxInFT);
    auto colIdx = 0u;
    for (auto& dataType : this->groupByHashKeysDataTypes) {
        auto size = Types::getDataTypeSize(dataType);
        tableSchema.appendColumn({isUnflat, dataChunkPos, size});
        hasStrCol = hasStrCol || dataType.typeID == STRING;
        compareFuncs[colIdx] = getCompareEntryWithKeysFunc(dataType.typeID);
        numBytesForGroupByHashKeys += size;
        colIdx++;
    }
    for (auto& dataType : this->groupByNonHashKeysDataTypes) {
        auto size = Types::getDataTypeSize(dataType);
        tableSchema.appendColumn({isUnflat, dataChunkPos, size});
        hasStrCol = hasStrCol || dataType.typeID == STRING;
        compareFuncs[colIdx] = getCompareEntryWithKeysFunc(dataType.typeID);
        numBytesForGroupByNonHashKeys += size;
        colIdx++;
    }
    aggStateColOffsetInFT = numBytesForGroupByHashKeys + numBytesForGroupByNonHashKeys;

    aggregateFunctions.resize(aggFuncs.size());
    updateAggFuncs.resize(aggFuncs.size());
    for (auto i = 0u; i < aggFuncs.size(); i++) {
        auto& aggFunc = aggFuncs[i];
        tableSchema.appendColumn({isUnflat, dataChunkPos, aggFunc->getAggregateStateSize()});
        aggregateFunctions[i] = aggFunc->clone();
        updateAggFuncs[i] = aggFunc->isFunctionDistinct() ?
                                &AggregateHashTable::updateDistinctAggState :
                                &AggregateHashTable::updateAggState;
    }
    factorizedTable = make_unique<FactorizedTable>(&memoryManager, tableSchema);
}

void AggregateHashTable::initializeTmpVectors() {
    hashState = make_shared<DataChunkState>();
    hashState->currIdx = 0;
    hashVector = make_unique<ValueVector>(&memoryManager, INT64);
    tmpHashVector = make_unique<ValueVector>(&memoryManager, INT64);
    hashVector->state = hashState;
    tmpHashVector->state = hashState;
    hashSlotsToUpdateAggState = make_unique<HashSlot*[]>(DEFAULT_VECTOR_CAPACITY);
    tmpValueIdxes = make_unique<uint64_t[]>(DEFAULT_VECTOR_CAPACITY);
    entryIdxesToInitialize = make_unique<uint64_t[]>(DEFAULT_VECTOR_CAPACITY);
    mayMatchIdxes = make_unique<uint64_t[]>(DEFAULT_VECTOR_CAPACITY);
    noMatchIdxes = make_unique<uint64_t[]>(DEFAULT_VECTOR_CAPACITY);
    tmpSlotIdxes = make_unique<uint64_t[]>(DEFAULT_VECTOR_CAPACITY);
}

// TODO(Ziyi): refactor the while loop in these two findEntryInDistinctHT functions.
uint8_t* AggregateHashTable::findEntry(uint8_t* entryBuffer, hash_t hash) {
    auto slotIdx = hash & bitMask;
    while (true) {
        auto slot = getHashSlot(slotIdx);
        if (slot->entry == nullptr) {
            return nullptr;
        } else if ((slot->hash == hash) && matchGroupByKeys(entryBuffer, slot->entry)) {
            return slot->entry;
        }
        increaseSlotIdx(slotIdx);
    }
}

uint8_t* AggregateHashTable::findEntryInDistinctHT(
    const vector<ValueVector*>& groupByKeyVectors, hash_t hash) {
    auto slotIdx = hash & bitMask;
    while (true) {
        auto slot = (HashSlot*)getHashSlot(slotIdx);
        if (slot->entry == nullptr) {
            return nullptr;
        } else if ((slot->hash == hash) && matchFlatGroupByKeys(groupByKeyVectors, slot->entry)) {
            return slot->entry;
        }
        increaseSlotIdx(slotIdx);
    }
}

void AggregateHashTable::initializeFTEntry(const vector<ValueVector*>& groupByFlatHashKeyVectors,
    const vector<ValueVector*>& groupByUnflatHashKeyVectors,
    const vector<ValueVector*>& groupByNonHashKeyVectors, uint32_t valueIdxInUnflatVector,
    uint8_t* entry) {
    auto colIdx = 0u;
    for (auto flatKeyVector : groupByFlatHashKeyVectors) {
        factorizedTable->updateFlatCell(
            entry, colIdx, flatKeyVector, flatKeyVector->state->getPositionOfCurrIdx());
        colIdx++;
    }
    for (auto unflatKeyVector : groupByUnflatHashKeyVectors) {
        factorizedTable->updateFlatCell(entry, colIdx, unflatKeyVector, valueIdxInUnflatVector);
        colIdx++;
    }
    for (auto nonHashKeyVector : groupByNonHashKeyVectors) {
        factorizedTable->updateFlatCell(entry, colIdx, nonHashKeyVector,
            nonHashKeyVector->state->isFlat() ? nonHashKeyVector->state->getPositionOfCurrIdx() :
                                                valueIdxInUnflatVector);
        colIdx++;
    }
    fillEntryWithInitialNullAggregateState(entry);
}

void AggregateHashTable::initializeFTEntries(const vector<ValueVector*>& groupByFlatHashKeyVectors,
    const vector<ValueVector*>& groupByUnflatHashKeyVectors,
    const vector<ValueVector*>& groupByNonHashKeyVectors, uint64_t numFTEntriesToUpdate) {
    for (auto i = 0u; i < numFTEntriesToUpdate; i++) {
        auto idx = entryIdxesToInitialize[i];
        initializeFTEntry(groupByFlatHashKeyVectors, groupByUnflatHashKeyVectors,
            groupByNonHashKeyVectors, idx, hashSlotsToUpdateAggState[idx]->entry);
    }
}

uint8_t* AggregateHashTable::createEntry(uint8_t* groupByKeys, hash_t hash) {
    auto groupByKeysAndAggregateStateBuffer = factorizedTable->appendEmptyTuple();
    auto nullMapOffset = factorizedTable->getTableSchema().getNullMapOffset();
    fillEntryWithGroupByKeys(groupByKeysAndAggregateStateBuffer, groupByKeys);
    fillEntryWithInitialNullAggregateState(groupByKeysAndAggregateStateBuffer);
    fillEntryWithNullMap(
        groupByKeysAndAggregateStateBuffer + nullMapOffset, groupByKeys + nullMapOffset);
    fillHashSlot(hash, groupByKeysAndAggregateStateBuffer);
    return groupByKeysAndAggregateStateBuffer;
}

uint8_t* AggregateHashTable::createEntryInDistinctHT(
    const vector<ValueVector*>& groupByHashKeyVectors, hash_t hash) {
    auto entry = factorizedTable->appendEmptyTuple();
    for (auto i = 0u; i < groupByHashKeyVectors.size(); i++) {
        factorizedTable->updateFlatCell(entry, i, groupByHashKeyVectors[i],
            groupByHashKeyVectors[i]->state->getPositionOfCurrIdx());
    }
    fillEntryWithInitialNullAggregateState(entry);
    fillHashSlot(hash, entry);
    return entry;
}

void AggregateHashTable::increaseSlotIdx(uint64_t& slotIdx) const {
    slotIdx++;
    if (slotIdx >= maxNumHashSlots) {
        slotIdx = 0;
    }
}

void AggregateHashTable::initTmpHashSlotsAndIdxes() {
    auto hashVal = (hash_t*)hashVector->values;
    if (hashVector->state->isFlat()) {
        auto pos = hashVector->state->getPositionOfCurrIdx();
        auto slotIdx = hashVal[pos] & bitMask;
        tmpSlotIdxes[pos] = slotIdx;
        hashSlotsToUpdateAggState[pos] = getHashSlot(slotIdx);
        tmpValueIdxes[0] = pos;
    } else {
        if (hashVector->state->isUnfiltered()) {
            for (auto i = 0u; i < hashVector->state->selectedSize; i++) {
                tmpValueIdxes[i] = i;
                tmpSlotIdxes[i] = hashVal[i] & bitMask;
                hashSlotsToUpdateAggState[i] = getHashSlot(tmpSlotIdxes[i]);
            }
        } else {
            for (auto i = 0u; i < hashVector->state->selectedSize; i++) {
                auto pos = hashVector->state->selectedPositions[i];
                tmpValueIdxes[i] = pos;
                tmpSlotIdxes[pos] = hashVal[pos] & bitMask;
                hashSlotsToUpdateAggState[pos] = getHashSlot(tmpSlotIdxes[pos]);
            }
        }
    }
}

void AggregateHashTable::increaseHashSlotIdxes(uint64_t numNoMatches) {
    for (auto i = 0u; i < numNoMatches; i++) {
        auto idx = noMatchIdxes[i];
        increaseSlotIdx(tmpSlotIdxes[idx]);
        hashSlotsToUpdateAggState[idx] = getHashSlot(tmpSlotIdxes[idx]);
    }
}

void AggregateHashTable::findHashSlots(const vector<ValueVector*>& groupByFlatHashKeyVectors,
    const vector<ValueVector*>& groupByUnflatHashKeyVectors,
    const vector<ValueVector*>& groupByNonHashKeyVectors) {
    initTmpHashSlotsAndIdxes();
    auto numEntriesToFindHashSlots = groupByUnflatHashKeyVectors.empty() ?
                                         1 :
                                         groupByUnflatHashKeyVectors[0]->state->selectedSize;
    while (numEntriesToFindHashSlots > 0) {
        auto numFTEntriesToUpdate = 0ul;
        auto numMayMatches = 0ul;
        auto numNoMatches = 0ul;
        for (auto i = 0u; i < numEntriesToFindHashSlots; i++) {
            auto idx = tmpValueIdxes[i];
            auto hash = ((hash_t*)hashVector->values)[idx];
            auto slot = hashSlotsToUpdateAggState[idx];
            if (slot->entry == nullptr) {
                entryIdxesToInitialize[numFTEntriesToUpdate++] = idx;
                slot->entry = factorizedTable->appendEmptyTuple();
                slot->hash = hash;
            } else if (slot->hash == hash) {
                mayMatchIdxes[numMayMatches++] = idx;
            } else {
                noMatchIdxes[numNoMatches++] = idx;
            }
        }
        initializeFTEntries(groupByFlatHashKeyVectors, groupByUnflatHashKeyVectors,
            groupByNonHashKeyVectors, numFTEntriesToUpdate);
        matchFTEntries(groupByFlatHashKeyVectors, groupByUnflatHashKeyVectors,
            groupByNonHashKeyVectors, numMayMatches, numNoMatches);
        increaseHashSlotIdxes(numNoMatches);
        numEntriesToFindHashSlots = numNoMatches;
        memcpy(tmpValueIdxes.get(), noMatchIdxes.get(), DEFAULT_VECTOR_CAPACITY * sizeof(uint64_t));
    }
}

void AggregateHashTable::computeAndCombineVecHash(
    const vector<ValueVector*>& groupByHashKeyVectors, uint32_t startVecIdx) {
    for (; startVecIdx < groupByHashKeyVectors.size(); startVecIdx++) {
        auto keyVector = groupByHashKeyVectors[startVecIdx];
        VectorHashOperations::computeHash(keyVector, tmpHashVector.get());
        VectorHashOperations::combineHash(tmpHashVector.get(), hashVector.get(), hashVector.get());
    }
}

void AggregateHashTable::computeVectorHashes(const vector<ValueVector*>& groupByFlatHashKeyVectors,
    const vector<ValueVector*>& groupByUnflatHashKeyVectors) {
    if (!groupByFlatHashKeyVectors.empty()) {
        VectorHashOperations::computeHash(groupByFlatHashKeyVectors[0], hashVector.get());
        computeAndCombineVecHash(groupByFlatHashKeyVectors, 1 /* startVecIdx */);
        computeAndCombineVecHash(groupByUnflatHashKeyVectors, 0 /* startVecIdx */);
    } else {
        VectorHashOperations::computeHash(groupByUnflatHashKeyVectors[0], hashVector.get());
        computeAndCombineVecHash(groupByUnflatHashKeyVectors, 1 /* startVecIdx */);
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

void AggregateHashTable::updateDistinctAggState(
    const vector<ValueVector*>& groupByFlatHashKeyVectors,
    const vector<ValueVector*>& groupByUnflatHashKeyVectors,
    unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggregateVector,
    uint64_t multiplicity, uint32_t colIdx, uint32_t aggStateOffset) {
    auto distinctHT = distinctHashTables[colIdx].get();
    assert(distinctHT != nullptr);
    if (distinctHT->isAggregateValueDistinctForGroupByKeys(
            groupByFlatHashKeyVectors, aggregateVector)) {
        auto pos = aggregateVector->state->getPositionOfCurrIdx();
        if (!aggregateVector->isNull(pos)) {
            aggregateFunction->updatePosState(
                hashSlotsToUpdateAggState[groupByFlatHashKeyVectors.empty() ?
                                              0 :
                                              groupByFlatHashKeyVectors[0]
                                                  ->state->getPositionOfCurrIdx()]
                        ->entry +
                    aggStateOffset,
                aggregateVector, 1 /* Distinct aggregate should ignore multiplicity
                                          since they are known to be non-distinct. */
                ,
                pos);
        }
    }
}

void AggregateHashTable::updateAggState(const vector<ValueVector*>& groupByFlatHashKeyVectors,
    const vector<ValueVector*>& groupByUnflatHashKeyVectors,
    unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector, uint64_t multiplicity,
    uint32_t colIdx, uint32_t aggStateOffset) {
    if (!aggVector) {
        updateNullAggVectorState(groupByFlatHashKeyVectors, groupByUnflatHashKeyVectors,
            aggregateFunction, multiplicity, aggStateOffset);
    } else if (aggVector->state->isFlat() && groupByUnflatHashKeyVectors.empty()) {
        updateBothFlatAggVectorState(
            groupByFlatHashKeyVectors, aggregateFunction, aggVector, multiplicity, aggStateOffset);
    } else if (aggVector->state->isFlat()) {
        updateFlatUnflatKeyFlatAggVectorState(groupByFlatHashKeyVectors,
            groupByUnflatHashKeyVectors, aggregateFunction, aggVector, multiplicity,
            aggStateOffset);
    } else if (groupByUnflatHashKeyVectors.empty()) {
        updateFlatKeyUnflatAggVectorState(
            groupByFlatHashKeyVectors, aggregateFunction, aggVector, multiplicity, aggStateOffset);
    } else if (!groupByUnflatHashKeyVectors.empty() &&
               (aggVector->state == groupByUnflatHashKeyVectors[0]->state)) {
        updateBothUnflatSameDCAggVectorState(groupByFlatHashKeyVectors, groupByUnflatHashKeyVectors,
            aggregateFunction, aggVector, multiplicity, aggStateOffset);
    } else {
        updateBothUnflatDifferentDCAggVectorState(groupByFlatHashKeyVectors,
            groupByUnflatHashKeyVectors, aggregateFunction, aggVector, multiplicity,
            aggStateOffset);
    }
}

void AggregateHashTable::updateAggStates(const vector<ValueVector*>& groupByFlatHashKeyVectors,
    const vector<ValueVector*>& groupByUnFlatHashKeyVectors,
    const vector<ValueVector*>& aggregateVectors, uint64_t multiplicity) {
    auto aggregateStateOffset = aggStateColOffsetInFT;
    for (auto i = 0u; i < aggregateFunctions.size(); i++) {
        updateAggFuncs[i](this, groupByFlatHashKeyVectors, groupByUnFlatHashKeyVectors,
            aggregateFunctions[i], aggregateVectors[i], multiplicity, i, aggregateStateOffset);
        aggregateStateOffset += aggregateFunctions[i]->getAggregateStateSize();
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
        if (!compareFuncs[i](keyValue, entry + factorizedTable->getTableSchema().getColOffset(i))) {
            return false;
        }
    }
    return true;
}

uint64_t AggregateHashTable::matchUnflatVecWithFTColumn(
    ValueVector* vector, uint64_t numMayMatches, uint64_t& numNoMatches, uint32_t colIdx) {
    assert(!vector->state->isFlat());
    auto colOffset = factorizedTable->getTableSchema().getColOffset(colIdx);
    auto mayMatchIdx = 0ul;
    if (vector->hasNoNullsGuarantee()) {
        for (auto i = 0u; i < numMayMatches; i++) {
            auto idx = mayMatchIdxes[i];
            auto value = vector->values + idx * vector->getNumBytesPerValue();
            auto isEntryKeyNull =
                FactorizedTable::isNull(hashSlotsToUpdateAggState[idx]->entry +
                                            factorizedTable->getTableSchema().getNullMapOffset(),
                    colIdx);
            if (isEntryKeyNull) {
                noMatchIdxes[numNoMatches++] = idx;
                continue;
            }
            if (compareFuncs[colIdx](value, hashSlotsToUpdateAggState[idx]->entry + colOffset)) {
                mayMatchIdxes[mayMatchIdx++] = idx;
            } else {
                noMatchIdxes[numNoMatches++] = idx;
            }
        }
    } else {
        for (auto i = 0u; i < numMayMatches; i++) {
            auto idx = mayMatchIdxes[i];
            auto value = vector->values + idx * vector->getNumBytesPerValue();
            auto isKeyVectorNull = vector->isNull(idx);
            auto isEntryKeyNull =
                FactorizedTable::isNull(hashSlotsToUpdateAggState[idx]->entry +
                                            factorizedTable->getTableSchema().getNullMapOffset(),
                    colIdx);
            if (isKeyVectorNull && isEntryKeyNull) {
                mayMatchIdxes[mayMatchIdx++] = idx;
                continue;
            } else if (isKeyVectorNull != isEntryKeyNull) {
                noMatchIdxes[numNoMatches++] = idx;
                continue;
            }

            if (compareFuncs[colIdx](value, hashSlotsToUpdateAggState[idx]->entry + colOffset)) {
                mayMatchIdxes[mayMatchIdx++] = idx;
            } else {
                noMatchIdxes[numNoMatches++] = idx;
            }
        }
    }
    return mayMatchIdx;
}

uint64_t AggregateHashTable::matchFlatVecWithFTColumn(
    ValueVector* vector, uint64_t numMayMatches, uint64_t& numNoMatches, uint32_t colIdx) {
    assert(vector->state->isFlat());
    auto colOffset = factorizedTable->getTableSchema().getColOffset(colIdx);
    auto mayMatchIdx = 0ul;
    auto pos = vector->state->currIdx;
    auto isVectorNull = vector->isNull(pos);
    auto value = vector->values + pos * vector->getNumBytesPerValue();
    for (auto i = 0u; i < numMayMatches; i++) {
        auto idx = mayMatchIdxes[i];
        auto isEntryKeyNull =
            FactorizedTable::isNull(hashSlotsToUpdateAggState[idx]->entry +
                                        factorizedTable->getTableSchema().getNullMapOffset(),
                colIdx);
        if (isEntryKeyNull && isVectorNull) {
            mayMatchIdxes[mayMatchIdx++] = idx;
            continue;
        } else if (isEntryKeyNull != isVectorNull) {
            noMatchIdxes[numNoMatches++] = idx;
            continue;
        }
        if (compareFuncs[colIdx](value, hashSlotsToUpdateAggState[idx]->entry + colOffset)) {
            mayMatchIdxes[mayMatchIdx++] = idx;
        } else {
            noMatchIdxes[numNoMatches++] = idx;
        }
    }
    return mayMatchIdx;
}

void AggregateHashTable::matchFTEntries(const vector<ValueVector*>& groupByFlatHashKeyVectors,
    const vector<ValueVector*>& groupByUnflatHashKeyVectors,
    const vector<ValueVector*>& groupByNonHashKeyVectors, uint64_t numMayMatches,
    uint64_t& numNoMatches) {
    auto colIdx = 0u;
    for (auto& groupByFlatHashKeyVector : groupByFlatHashKeyVectors) {
        numMayMatches = matchFlatVecWithFTColumn(
            groupByFlatHashKeyVector, numMayMatches, numNoMatches, colIdx++);
    }

    for (auto& groupByUnflatHashKeyVector : groupByUnflatHashKeyVectors) {
        numMayMatches = matchUnflatVecWithFTColumn(
            groupByUnflatHashKeyVector, numMayMatches, numNoMatches, colIdx++);
    }

    for (auto& groupByNonHashKeyVector : groupByNonHashKeyVectors) {
        if (groupByNonHashKeyVector->state->isFlat()) {
            numMayMatches = matchFlatVecWithFTColumn(
                groupByNonHashKeyVector, numMayMatches, numNoMatches, colIdx++);
        } else {
            numMayMatches = matchUnflatVecWithFTColumn(
                groupByNonHashKeyVector, numMayMatches, numNoMatches, colIdx++);
        }
    }
}

bool AggregateHashTable::matchGroupByKeys(uint8_t* entryBuffer, uint8_t* entryBufferToMatch) {
    auto nullMapOffset = factorizedTable->getTableSchema().getNullMapOffset();
    if (memcmp(entryBuffer + nullMapOffset, entryBufferToMatch + nullMapOffset,
            factorizedTable->getTableSchema().getNumBytesForNullMap()) != 0) {
        return false;
    }
    if (hasStrCol) {
        for (auto i = 0u; i < groupByHashKeysDataTypes.size(); i++) {
            auto colOffset = factorizedTable->getTableSchema().getColOffset(i);
            if (!compareFuncs[i](entryBuffer + colOffset, entryBufferToMatch + colOffset)) {
                return false;
            }
        }
        return true;
    } else {
        return memcmp(entryBufferToMatch, entryBuffer, numBytesForGroupByHashKeys) == 0;
    }
}

void AggregateHashTable::fillEntryWithInitialNullAggregateState(uint8_t* entry) {
    for (auto i = 0u; i < aggregateFunctions.size(); i++) {
        factorizedTable->updateFlatCell(entry, aggStateColIdxInFT + i,
            (void*)aggregateFunctions[i]->getInitialNullAggregateState());
    }
}

void AggregateHashTable::fillHashSlot(hash_t hash, uint8_t* groupByKeysAndAggregateStateBuffer) {
    auto slotIdx = hash & bitMask;
    while (getHashSlot(slotIdx)->entry) {
        increaseSlotIdx(slotIdx);
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

void AggregateHashTable::resizeHashTableIfNecessary() {
    if (factorizedTable->getNumTuples() > maxNumHashSlots ||
        (double)factorizedTable->getNumTuples() >
            (double)maxNumHashSlots / DEFAULT_HT_LOAD_FACTOR) {
        resize(maxNumHashSlots * 2);
    }
}

template<typename type>
bool AggregateHashTable::compareEntryWithKeys(const uint8_t* keyValue, const uint8_t* entry) {
    uint8_t result;
    Equals::operation(*(type*)keyValue, *(type*)entry, result);
    return result != 0;
}

compare_function_t AggregateHashTable::getCompareEntryWithKeysFunc(DataTypeID typeId) {
    switch (typeId) {
    case NODE_ID: {
        return compareEntryWithKeys<nodeID_t>;
    }
    case BOOL: {
        return compareEntryWithKeys<bool>;
    }
    case INT64: {
        return compareEntryWithKeys<int64_t>;
    }
    case DOUBLE: {
        return compareEntryWithKeys<double_t>;
    }
    case STRING: {
        return compareEntryWithKeys<gf_string_t>;
    }
    case DATE: {
        return compareEntryWithKeys<date_t>;
    }
    case TIMESTAMP: {
        return compareEntryWithKeys<timestamp_t>;
    }
    case INTERVAL: {
        return compareEntryWithKeys<interval_t>;
    }
    case UNSTRUCTURED: {
        return compareEntryWithKeys<Value>;
    }
    default: {
        throw RuntimeException("Cannot compare data type " + Types::dataTypeToString(typeId));
    }
    }
}

void AggregateHashTable::updateNullAggVectorState(
    const vector<ValueVector*>& groupByFlatHashKeyVectors,
    const vector<ValueVector*>& groupByUnflatHashKeyVectors,
    unique_ptr<AggregateFunction>& aggregateFunction, uint64_t multiplicity,
    uint32_t aggStateOffset) {
    if (groupByUnflatHashKeyVectors.empty()) {
        auto pos = groupByFlatHashKeyVectors[0]->state->getPositionOfCurrIdx();
        aggregateFunction->updatePosState(hashSlotsToUpdateAggState[pos]->entry + aggStateOffset,
            nullptr, multiplicity, 0 /* dummy pos */);
    } else if (groupByUnflatHashKeyVectors[0]->state->isUnfiltered()) {
        auto selectedSize = groupByUnflatHashKeyVectors[0]->state->selectedSize;
        for (auto i = 0u; i < selectedSize; i++) {
            aggregateFunction->updatePosState(hashSlotsToUpdateAggState[i]->entry + aggStateOffset,
                nullptr, multiplicity, 0 /* dummy pos */);
        }
    } else {
        auto selectedSize = groupByUnflatHashKeyVectors[0]->state->selectedSize;
        for (auto i = 0u; i < selectedSize; i++) {
            auto pos = groupByUnflatHashKeyVectors[0]->state->selectedPositions[i];
            aggregateFunction->updatePosState(
                hashSlotsToUpdateAggState[pos]->entry + aggStateOffset, nullptr, multiplicity,
                0 /* dummy pos */);
        }
    }
}

void AggregateHashTable::updateBothFlatAggVectorState(
    const vector<ValueVector*>& groupByFlatHashKeyVectors,
    unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector, uint64_t multiplicity,
    uint32_t aggStateOffset) {
    auto aggPos = aggVector->state->getPositionOfCurrIdx();
    if (!aggVector->isNull(aggPos)) {
        aggregateFunction->updatePosState(
            hashSlotsToUpdateAggState[groupByFlatHashKeyVectors[0]->state->getPositionOfCurrIdx()]
                    ->entry +
                aggStateOffset,
            aggVector, multiplicity, aggPos);
    }
}

void AggregateHashTable::updateFlatUnflatKeyFlatAggVectorState(
    const vector<ValueVector*>& groupByFlatHashKeyVectors,
    const vector<ValueVector*>& groupByUnflatHashKeyVectors,
    unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector, uint64_t multiplicity,
    uint32_t aggStateOffset) {
    auto aggPos = aggVector->state->getPositionOfCurrIdx();
    auto selectedSize = groupByUnflatHashKeyVectors[0]->state->selectedSize;
    if (!aggVector->isNull(aggPos)) {
        if (groupByUnflatHashKeyVectors[0]->state->isUnfiltered()) {
            for (auto i = 0u; i < selectedSize; i++) {
                aggregateFunction->updatePosState(
                    hashSlotsToUpdateAggState[i]->entry + aggStateOffset, aggVector, multiplicity,
                    aggPos);
            }
        } else {
            for (auto i = 0u; i < selectedSize; i++) {
                auto pos = groupByUnflatHashKeyVectors[0]->state->selectedPositions[i];
                aggregateFunction->updatePosState(
                    hashSlotsToUpdateAggState[pos]->entry + aggStateOffset, aggVector, multiplicity,
                    aggPos);
            }
        }
    }
}

void AggregateHashTable::updateFlatKeyUnflatAggVectorState(
    const vector<ValueVector*>& groupByFlatHashKeyVectors,
    unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector, uint64_t multiplicity,
    uint32_t aggStateOffset) {
    auto groupByKeyPos = groupByFlatHashKeyVectors[0]->state->getPositionOfCurrIdx();
    auto aggVecSelectedSize = aggVector->state->selectedSize;
    if (aggVector->hasNoNullsGuarantee()) {
        if (aggVector->state->isUnfiltered()) {
            for (auto i = 0u; i < aggVecSelectedSize; i++) {
                aggregateFunction->updatePosState(
                    hashSlotsToUpdateAggState[groupByKeyPos]->entry + aggStateOffset, aggVector,
                    multiplicity, aggVector->state->selectedPositions[i]);
            }
        } else {
            for (auto i = 0u; i < aggVecSelectedSize; i++) {
                aggregateFunction->updatePosState(
                    hashSlotsToUpdateAggState[groupByKeyPos]->entry + aggStateOffset, aggVector,
                    multiplicity, aggVector->state->selectedPositions[i]);
            }
        }
    } else {
        if (aggVector->state->isUnfiltered()) {
            for (auto i = 0u; i < aggVecSelectedSize; i++) {
                if (!aggVector->isNull(i)) {
                    aggregateFunction->updatePosState(
                        hashSlotsToUpdateAggState[0]->entry + aggStateOffset, aggVector,
                        multiplicity, i);
                }
            }
        } else {
            for (auto i = 0u; i < aggVecSelectedSize; i++) {
                auto pos = aggVector->state->selectedPositions[i];
                if (!aggVector->isNull(pos)) {
                    aggregateFunction->updatePosState(
                        hashSlotsToUpdateAggState[0]->entry + aggStateOffset, aggVector,
                        multiplicity, pos);
                }
            }
        }
    }
}

void AggregateHashTable::updateBothUnflatSameDCAggVectorState(
    const vector<ValueVector*>& groupByFlatHashKeyVectors,
    const vector<ValueVector*>& groupByUnflatHashKeyVectors,
    unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector, uint64_t multiplicity,
    uint32_t aggStateOffset) {
    if (aggVector->hasNoNullsGuarantee()) {
        if (aggVector->state->isUnfiltered()) {
            for (auto i = 0u; i < aggVector->state->selectedSize; i++) {
                aggregateFunction->updatePosState(
                    hashSlotsToUpdateAggState[i]->entry + aggStateOffset, aggVector, multiplicity,
                    i);
            }
        } else {
            for (auto i = 0u; i < aggVector->state->selectedSize; i++) {
                auto pos = aggVector->state->selectedPositions[i];
                aggregateFunction->updatePosState(
                    hashSlotsToUpdateAggState[pos]->entry + aggStateOffset, aggVector, multiplicity,
                    pos);
            }
        }
    } else {
        if (aggVector->state->isUnfiltered()) {
            for (auto i = 0u; i < aggVector->state->selectedSize; i++) {
                if (!aggVector->isNull(i)) {
                    aggregateFunction->updatePosState(
                        hashSlotsToUpdateAggState[i]->entry + aggStateOffset, aggVector,
                        multiplicity, i);
                }
            }
        } else {
            for (auto i = 0u; i < aggVector->state->selectedSize; i++) {
                auto pos = aggVector->state->selectedPositions[i];
                if (!aggVector->isNull(pos)) {
                    aggregateFunction->updatePosState(
                        hashSlotsToUpdateAggState[pos]->entry + aggStateOffset, aggVector,
                        multiplicity, pos);
                }
            }
        }
    }
}

void AggregateHashTable::updateBothUnflatDifferentDCAggVectorState(
    const vector<ValueVector*>& groupByFlatHashKeyVectors,
    const vector<ValueVector*>& groupByUnflatHashKeyVectors,
    unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector, uint64_t multiplicity,
    uint32_t aggStateOffset) {
    auto selectedSize = groupByUnflatHashKeyVectors[0]->state->selectedSize;
    if (groupByUnflatHashKeyVectors[0]->state->isUnfiltered()) {
        for (auto i = 0u; i < selectedSize; i++) {
            aggregateFunction->updateAllState(
                hashSlotsToUpdateAggState[i]->entry + aggStateOffset, aggVector, multiplicity);
        }
    } else {
        for (auto i = 0u; i < selectedSize; i++) {
            auto pos = groupByUnflatHashKeyVectors[0]->state->selectedPositions[i];
            aggregateFunction->updateAllState(
                hashSlotsToUpdateAggState[pos]->entry + aggStateOffset, aggVector, multiplicity);
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
