#include "processor/operator/aggregate/aggregate_hash_table.h"

#include "common/utils.h"

using namespace kuzu::common;
using namespace kuzu::function;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

AggregateHashTable::AggregateHashTable(MemoryManager& memoryManager,
    std::vector<LogicalType> keyTypes, std::vector<LogicalType> payloadTypes,
    const std::vector<std::unique_ptr<AggregateFunction>>& aggregateFunctions,
    const std::vector<LogicalType>& distinctAggKeyTypes, uint64_t numEntriesToAllocate,
    FactorizedTableSchema tableSchema)
    : BaseHashTable{memoryManager, std::move(keyTypes)}, payloadTypes{std::move(payloadTypes)} {
    initializeFT(aggregateFunctions, std::move(tableSchema));
    initializeHashTable(numEntriesToAllocate);
    KU_ASSERT(aggregateFunctions.size() == distinctAggKeyTypes.size());
    for (auto i = 0u; i < this->aggregateFunctions.size(); ++i) {
        std::unique_ptr<AggregateHashTable> distinctHT;
        if (this->aggregateFunctions[i]->isDistinct) {
            distinctHT = AggregateHashTableUtils::createDistinctHashTable(memoryManager,
                this->keyTypes, distinctAggKeyTypes[i]);
        } else {
            distinctHT = nullptr;
        }
        distinctHashTables.push_back(std::move(distinctHT));
    }
    initializeTmpVectors();
}

void AggregateHashTable::append(const std::vector<ValueVector*>& flatKeyVectors,
    const std::vector<ValueVector*>& unFlatKeyVectors,
    const std::vector<ValueVector*>& dependentKeyVectors, DataChunkState* leadingState,
    const std::vector<AggregateInput>& aggregateInputs, uint64_t resultSetMultiplicity) {
    resizeHashTableIfNecessary(leadingState->getSelVector().getSelSize());
    computeVectorHashes(flatKeyVectors, unFlatKeyVectors);
    findHashSlots(flatKeyVectors, unFlatKeyVectors, dependentKeyVectors, leadingState);
    updateAggStates(flatKeyVectors, unFlatKeyVectors, aggregateInputs, resultSetMultiplicity);
}

bool AggregateHashTable::isAggregateValueDistinctForGroupByKeys(
    const std::vector<ValueVector*>& groupByFlatKeyVectors, ValueVector* aggregateVector) {
    std::vector<ValueVector*> distinctKeyVectors(groupByFlatKeyVectors.size() + 1);
    for (auto i = 0u; i < groupByFlatKeyVectors.size(); i++) {
        distinctKeyVectors[i] = groupByFlatKeyVectors[i];
    }
    distinctKeyVectors[groupByFlatKeyVectors.size()] = aggregateVector;
    computeVectorHashes(distinctKeyVectors, std::vector<ValueVector*>() /* unFlatKeyVectors */);
    auto hash = hashVector->getValue<hash_t>(hashVector->state->getSelVector()[0]);
    auto distinctHTEntry = findEntryInDistinctHT(distinctKeyVectors, hash);
    if (distinctHTEntry == nullptr) {
        resizeHashTableIfNecessary(1);
        createEntryInDistinctHT(distinctKeyVectors, hash);
        return true;
    }
    return false;
}

void AggregateHashTable::merge(AggregateHashTable& other) {
    std::shared_ptr<DataChunkState> vectorsToScanState = std::make_shared<DataChunkState>();
    std::vector<ValueVector*> vectorsToScan(keyTypes.size() + payloadTypes.size());
    std::vector<ValueVector*> groupByHashVectors(keyTypes.size());
    std::vector<ValueVector*> groupByNonHashVectors(payloadTypes.size());
    std::vector<std::unique_ptr<ValueVector>> hashKeyVectors(keyTypes.size());
    std::vector<std::unique_ptr<ValueVector>> nonHashKeyVectors(groupByNonHashVectors.size());
    for (auto i = 0u; i < keyTypes.size(); i++) {
        auto hashKeyVec = std::make_unique<ValueVector>(keyTypes[i].copy(), &memoryManager);
        hashKeyVec->state = vectorsToScanState;
        vectorsToScan[i] = hashKeyVec.get();
        groupByHashVectors[i] = hashKeyVec.get();
        hashKeyVectors[i] = std::move(hashKeyVec);
    }
    for (auto i = 0u; i < payloadTypes.size(); i++) {
        auto nonHashKeyVec = std::make_unique<ValueVector>(payloadTypes[i].copy(), &memoryManager);
        nonHashKeyVec->state = vectorsToScanState;
        vectorsToScan[i + keyTypes.size()] = nonHashKeyVec.get();
        groupByNonHashVectors[i] = nonHashKeyVec.get();
        nonHashKeyVectors[i] = std::move(nonHashKeyVec);
    }
    hashVector->state = vectorsToScanState;
    hashVector->setAllNonNull();
    vectorsToScan.emplace_back(hashVector.get());

    std::vector<uint32_t> colIdxesToScan(vectorsToScan.size() - 1);
    iota(colIdxesToScan.begin(), colIdxesToScan.end(), 0);
    // Note: we store hash values at the last column of factorizedTable.
    colIdxesToScan.push_back(factorizedTable->getTableSchema()->getNumColumns() - 1);
    uint64_t startTupleIdx = 0;
    while (startTupleIdx < other.factorizedTable->getNumTuples()) {
        auto numTuplesToScan = std::min(other.factorizedTable->getNumTuples() - startTupleIdx,
            DEFAULT_VECTOR_CAPACITY);
        other.factorizedTable->scan(vectorsToScan, startTupleIdx, numTuplesToScan, colIdxesToScan);
        findHashSlots(std::vector<ValueVector*>(), groupByHashVectors, groupByNonHashVectors,
            vectorsToScanState.get());
        auto aggregateStateOffset = aggStateColOffsetInFT;
        for (auto& aggregateFunction : aggregateFunctions) {
            for (auto i = 0u; i < numTuplesToScan; i++) {
                aggregateFunction->combineState(hashSlotsToUpdateAggState[i]->entry +
                                                    aggregateStateOffset,
                    other.factorizedTable->getTuple(startTupleIdx + i) + aggregateStateOffset,
                    &memoryManager);
            }
            aggregateStateOffset += aggregateFunction->getAggregateStateSize();
        }
        startTupleIdx += numTuplesToScan;
    }
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

void AggregateHashTable::initializeFT(
    const std::vector<std::unique_ptr<AggregateFunction>>& aggFuncs,
    FactorizedTableSchema tableSchema) {
    aggStateColIdxInFT = keyTypes.size() + payloadTypes.size();
    for (auto& dataType : keyTypes) {
        numBytesForKeys += LogicalTypeUtils::getRowLayoutSize(dataType);
    }
    for (auto& dataType : payloadTypes) {
        numBytesForDependentKeys += LogicalTypeUtils::getRowLayoutSize(dataType);
    }
    aggStateColOffsetInFT = numBytesForKeys + numBytesForDependentKeys;

    aggregateFunctions.reserve(aggFuncs.size());
    updateAggFuncs.reserve(aggFuncs.size());
    for (auto i = 0u; i < aggFuncs.size(); i++) {
        auto& aggFunc = aggFuncs[i];
        aggregateFunctions.push_back(aggFunc->clone());
        updateAggFuncs.push_back(aggFunc->isFunctionDistinct() ?
                                     &AggregateHashTable::updateDistinctAggState :
                                     &AggregateHashTable::updateAggState);
    }
    hashColIdxInFT = tableSchema.getNumColumns() - 1;
    hashColOffsetInFT = tableSchema.getColOffset(hashColIdxInFT);
    factorizedTable = std::make_unique<FactorizedTable>(&memoryManager, std::move(tableSchema));
}

void AggregateHashTable::initializeHashTable(uint64_t numEntriesToAllocate) {
    auto numHashSlotsPerBlock = prevPowerOfTwo(HASH_BLOCK_SIZE / sizeof(HashSlot));
    setMaxNumHashSlots(nextPowerOfTwo(std::max(numHashSlotsPerBlock, numEntriesToAllocate)));
    initSlotConstant(numHashSlotsPerBlock);
    auto numDataBlocks =
        maxNumHashSlots / numHashSlotsPerBlock + (maxNumHashSlots % numHashSlotsPerBlock != 0);
    for (auto i = 0u; i < numDataBlocks; i++) {
        hashSlotsBlocks.emplace_back(std::make_unique<DataBlock>(&memoryManager, HASH_BLOCK_SIZE));
    }
}

void AggregateHashTable::initializeTmpVectors() {
    hashSlotsToUpdateAggState = std::make_unique<HashSlot*[]>(DEFAULT_VECTOR_CAPACITY);
    tmpValueIdxes = std::make_unique<uint64_t[]>(DEFAULT_VECTOR_CAPACITY);
    entryIdxesToInitialize = std::make_unique<uint64_t[]>(DEFAULT_VECTOR_CAPACITY);
    mayMatchIdxes = std::make_unique<uint64_t[]>(DEFAULT_VECTOR_CAPACITY);
    noMatchIdxes = std::make_unique<uint64_t[]>(DEFAULT_VECTOR_CAPACITY);
    tmpSlotIdxes = std::make_unique<uint64_t[]>(DEFAULT_VECTOR_CAPACITY);
}

uint8_t* AggregateHashTable::findEntryInDistinctHT(
    const std::vector<ValueVector*>& groupByKeyVectors, hash_t hash) {
    auto slotIdx = getSlotIdxForHash(hash);
    while (true) {
        auto slot = (HashSlot*)getHashSlot(slotIdx);
        if (slot->entry == nullptr) {
            return nullptr;
        } else if ((slot->hash == hash) && matchFlatVecWithEntry(groupByKeyVectors, slot->entry)) {
            return slot->entry;
        }
        increaseSlotIdx(slotIdx);
    }
}

void AggregateHashTable::resize(uint64_t newSize) {
    setMaxNumHashSlots(newSize);
    addDataBlocksIfNecessary(maxNumHashSlots);
    for (auto& block : hashSlotsBlocks) {
        block->resetToZero();
    }
    for (auto& tupleBlock : factorizedTable->getTupleDataBlocks()) {
        uint8_t* tuple = tupleBlock->getData();
        for (auto i = 0u; i < tupleBlock->numTuples; i++) {
            fillHashSlot(*(hash_t*)(tuple + hashColOffsetInFT), tuple);
            tuple += factorizedTable->getTableSchema()->getNumBytesPerTuple();
        }
    }
}

uint64_t AggregateHashTable::matchFTEntries(const std::vector<ValueVector*>& flatKeyVectors,
    const std::vector<ValueVector*>& unFlatKeyVectors, uint64_t numMayMatches,
    uint64_t numNoMatches) {
    auto colIdx = 0u;
    for (auto& flatKeyVector : flatKeyVectors) {
        numMayMatches =
            matchFlatVecWithFTColumn(flatKeyVector, numMayMatches, numNoMatches, colIdx++);
    }
    for (auto& unFlatKeyVector : unFlatKeyVectors) {
        numMayMatches =
            matchUnFlatVecWithFTColumn(unFlatKeyVector, numMayMatches, numNoMatches, colIdx++);
    }
    return numNoMatches;
}

void AggregateHashTable::initializeFTEntries(const std::vector<ValueVector*>& flatKeyVectors,
    const std::vector<ValueVector*>& unFlatKeyVectors,
    const std::vector<ValueVector*>& dependentKeyVectors, uint64_t numFTEntriesToInitialize) {
    auto colIdx = 0u;
    for (auto flatKeyVector : flatKeyVectors) {
        initializeFTEntryWithFlatVec(flatKeyVector, numFTEntriesToInitialize, colIdx++);
    }
    for (auto unFlatKeyVector : unFlatKeyVectors) {
        initializeFTEntryWithUnFlatVec(unFlatKeyVector, numFTEntriesToInitialize, colIdx++);
    }
    for (auto dependentKeyVector : dependentKeyVectors) {
        if (dependentKeyVector->state->isFlat()) {
            initializeFTEntryWithFlatVec(dependentKeyVector, numFTEntriesToInitialize, colIdx++);
        } else {
            initializeFTEntryWithUnFlatVec(dependentKeyVector, numFTEntriesToInitialize, colIdx++);
        }
    }
    for (auto i = 0u; i < numFTEntriesToInitialize; i++) {
        auto entryIdx = entryIdxesToInitialize[i];
        auto entry = hashSlotsToUpdateAggState[entryIdx]->entry;
        fillEntryWithInitialNullAggregateState(entry);
        // Fill the hashValue in the ftEntry.
        factorizedTable->updateFlatCellNoNull(entry, hashColIdxInFT,
            hashVector->getData() + hashVector->getNumBytesPerValue() * entryIdx);
    }
}

uint64_t AggregateHashTable::matchUnFlatVecWithFTColumn(ValueVector* vector, uint64_t numMayMatches,
    uint64_t& numNoMatches, uint32_t colIdx) {
    KU_ASSERT(!vector->state->isFlat());
    auto colOffset = factorizedTable->getTableSchema()->getColOffset(colIdx);
    uint64_t mayMatchIdx = 0;
    if (vector->hasNoNullsGuarantee()) {
        if (factorizedTable->hasNoNullGuarantee(colIdx)) {
            for (auto i = 0u; i < numMayMatches; i++) {
                auto idx = mayMatchIdxes[i];
                if (compareEntryFuncs[colIdx](vector, idx,
                        hashSlotsToUpdateAggState[idx]->entry + colOffset)) {
                    mayMatchIdxes[mayMatchIdx++] = idx;
                } else {
                    noMatchIdxes[numNoMatches++] = idx;
                }
            }
        } else {
            for (auto i = 0u; i < numMayMatches; i++) {
                auto idx = mayMatchIdxes[i];
                auto isEntryKeyNull = factorizedTable->isNonOverflowColNull(
                    hashSlotsToUpdateAggState[idx]->entry +
                        factorizedTable->getTableSchema()->getNullMapOffset(),
                    colIdx);
                if (isEntryKeyNull) {
                    noMatchIdxes[numNoMatches++] = idx;
                    continue;
                }
                if (compareEntryFuncs[colIdx](vector, idx,
                        hashSlotsToUpdateAggState[idx]->entry + colOffset)) {
                    mayMatchIdxes[mayMatchIdx++] = idx;
                } else {
                    noMatchIdxes[numNoMatches++] = idx;
                }
            }
        }
    } else {
        for (auto i = 0u; i < numMayMatches; i++) {
            auto idx = mayMatchIdxes[i];
            auto isKeyVectorNull = vector->isNull(idx);
            auto isEntryKeyNull = factorizedTable->isNonOverflowColNull(
                hashSlotsToUpdateAggState[idx]->entry +
                    factorizedTable->getTableSchema()->getNullMapOffset(),
                colIdx);
            if (isKeyVectorNull && isEntryKeyNull) {
                mayMatchIdxes[mayMatchIdx++] = idx;
                continue;
            } else if (isKeyVectorNull != isEntryKeyNull) {
                noMatchIdxes[numNoMatches++] = idx;
                continue;
            }

            if (compareEntryFuncs[colIdx](vector, idx,
                    hashSlotsToUpdateAggState[idx]->entry + colOffset)) {
                mayMatchIdxes[mayMatchIdx++] = idx;
            } else {
                noMatchIdxes[numNoMatches++] = idx;
            }
        }
    }
    return mayMatchIdx;
}

uint64_t AggregateHashTable::matchFlatVecWithFTColumn(ValueVector* vector, uint64_t numMayMatches,
    uint64_t& numNoMatches, uint32_t colIdx) {
    KU_ASSERT(vector->state->isFlat());
    auto colOffset = factorizedTable->getTableSchema()->getColOffset(colIdx);
    uint64_t mayMatchIdx = 0;
    auto pos = vector->state->getSelVector()[0];
    auto isVectorNull = vector->isNull(pos);
    for (auto i = 0u; i < numMayMatches; i++) {
        auto idx = mayMatchIdxes[i];
        auto isEntryKeyNull = factorizedTable->isNonOverflowColNull(
            hashSlotsToUpdateAggState[idx]->entry +
                factorizedTable->getTableSchema()->getNullMapOffset(),
            colIdx);
        if (isEntryKeyNull && isVectorNull) {
            mayMatchIdxes[mayMatchIdx++] = idx;
            continue;
        } else if (isEntryKeyNull != isVectorNull) {
            noMatchIdxes[numNoMatches++] = idx;
            continue;
        }
        if (compareEntryFuncs[colIdx](vector, pos,
                hashSlotsToUpdateAggState[idx]->entry + colOffset)) {
            mayMatchIdxes[mayMatchIdx++] = idx;
        } else {
            noMatchIdxes[numNoMatches++] = idx;
        }
    }
    return mayMatchIdx;
}

void AggregateHashTable::initializeFTEntryWithFlatVec(ValueVector* flatVector,
    uint64_t numEntriesToInitialize, uint32_t colIdx) {
    KU_ASSERT(flatVector->state->isFlat());
    auto colOffset = factorizedTable->getTableSchema()->getColOffset(colIdx);
    auto pos = flatVector->state->getSelVector()[0];
    if (flatVector->isNull(pos)) {
        for (auto i = 0u; i < numEntriesToInitialize; i++) {
            auto idx = entryIdxesToInitialize[i];
            auto entry = hashSlotsToUpdateAggState[idx]->entry;
            factorizedTable->setNonOverflowColNull(
                entry + factorizedTable->getTableSchema()->getNullMapOffset(), colIdx);
        }
    } else {
        for (auto i = 0u; i < numEntriesToInitialize; i++) {
            auto idx = entryIdxesToInitialize[i];
            auto entry = hashSlotsToUpdateAggState[idx]->entry;
            flatVector->copyToRowData(pos, entry + colOffset,
                factorizedTable->getInMemOverflowBuffer());
        }
    }
}

void AggregateHashTable::initializeFTEntryWithUnFlatVec(ValueVector* unFlatVector,
    uint64_t numEntriesToInitialize, uint32_t colIdx) {
    KU_ASSERT(!unFlatVector->state->isFlat());
    auto colOffset = factorizedTable->getTableSchema()->getColOffset(colIdx);
    if (unFlatVector->hasNoNullsGuarantee()) {
        for (auto i = 0u; i < numEntriesToInitialize; i++) {
            auto entryIdx = entryIdxesToInitialize[i];
            unFlatVector->copyToRowData(entryIdx,
                hashSlotsToUpdateAggState[entryIdx]->entry + colOffset,
                factorizedTable->getInMemOverflowBuffer());
        }
    } else {
        for (auto i = 0u; i < numEntriesToInitialize; i++) {
            auto entryIdx = entryIdxesToInitialize[i];
            factorizedTable->updateFlatCell(hashSlotsToUpdateAggState[entryIdx]->entry, colIdx,
                unFlatVector, entryIdx);
        }
    }
}

uint8_t* AggregateHashTable::createEntryInDistinctHT(
    const std::vector<ValueVector*>& groupByHashKeyVectors, hash_t hash) {
    auto entry = factorizedTable->appendEmptyTuple();
    for (auto i = 0u; i < groupByHashKeyVectors.size(); i++) {
        factorizedTable->updateFlatCell(entry, i, groupByHashKeyVectors[i],
            groupByHashKeyVectors[i]->state->getSelVector()[0]);
    }
    factorizedTable->updateFlatCellNoNull(entry, hashColIdxInFT, &hash);
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
    auto& hashSelVector = hashVector->state->getSelVector();
    if (hashSelVector.isUnfiltered()) {
        for (auto i = 0u; i < hashSelVector.getSelSize(); i++) {
            tmpValueIdxes[i] = i;
            tmpSlotIdxes[i] = getSlotIdxForHash(hashVector->getValue<hash_t>(i));
            hashSlotsToUpdateAggState[i] = getHashSlot(tmpSlotIdxes[i]);
        }
    } else {
        for (auto i = 0u; i < hashSelVector.getSelSize(); i++) {
            auto pos = hashSelVector[i];
            tmpValueIdxes[i] = pos;
            tmpSlotIdxes[pos] = getSlotIdxForHash(hashVector->getValue<hash_t>(pos));
            hashSlotsToUpdateAggState[pos] = getHashSlot(tmpSlotIdxes[pos]);
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

void AggregateHashTable::findHashSlots(const std::vector<ValueVector*>& flatKeyVectors,
    const std::vector<ValueVector*>& unFlatKeyVectors,
    const std::vector<ValueVector*>& dependentKeyVectors, DataChunkState* leadingState) {
    initTmpHashSlotsAndIdxes();
    auto numEntriesToFindHashSlots = leadingState->getSelVector().getSelSize();
    while (numEntriesToFindHashSlots > 0) {
        uint64_t numFTEntriesToUpdate = 0;
        uint64_t numMayMatches = 0;
        uint64_t numNoMatches = 0;
        for (auto i = 0u; i < numEntriesToFindHashSlots; i++) {
            auto idx = tmpValueIdxes[i];
            auto hash = hashVector->getValue<hash_t>(idx);
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
        initializeFTEntries(flatKeyVectors, unFlatKeyVectors, dependentKeyVectors,
            numFTEntriesToUpdate);
        numNoMatches =
            matchFTEntries(flatKeyVectors, unFlatKeyVectors, numMayMatches, numNoMatches);
        increaseHashSlotIdxes(numNoMatches);
        numEntriesToFindHashSlots = numNoMatches;
        memcpy(tmpValueIdxes.get(), noMatchIdxes.get(), DEFAULT_VECTOR_CAPACITY * sizeof(uint64_t));
    }
}

void AggregateHashTable::updateDistinctAggState(const std::vector<ValueVector*>& flatKeyVectors,
    const std::vector<ValueVector*>& /*unFlatKeyVectors*/,
    std::unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggregateVector,
    uint64_t /*multiplicity*/, uint32_t colIdx, uint32_t aggStateOffset) {
    auto distinctHT = distinctHashTables[colIdx].get();
    KU_ASSERT(distinctHT != nullptr);
    if (distinctHT->isAggregateValueDistinctForGroupByKeys(flatKeyVectors, aggregateVector)) {
        auto pos = aggregateVector->state->getSelVector()[0];
        if (!aggregateVector->isNull(pos)) {
            aggregateFunction->updatePosState(
                hashSlotsToUpdateAggState[flatKeyVectors.empty() ?
                                              0 :
                                              flatKeyVectors[0]->state->getSelVector()[0]]
                        ->entry +
                    aggStateOffset,
                aggregateVector, 1 /* Distinct aggregate should ignore multiplicity
                                          since they are known to be non-distinct. */
                ,
                pos, &memoryManager);
        }
    }
}

void AggregateHashTable::updateAggState(const std::vector<ValueVector*>& flatKeyVectors,
    const std::vector<ValueVector*>& unFlatKeyVectors,
    std::unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector,
    uint64_t multiplicity, uint32_t /*colIdx*/, uint32_t aggStateOffset) {
    if (!aggVector) {
        updateNullAggVectorState(flatKeyVectors, unFlatKeyVectors, aggregateFunction, multiplicity,
            aggStateOffset);
    } else if (aggVector->state->isFlat() && unFlatKeyVectors.empty()) {
        updateBothFlatAggVectorState(flatKeyVectors, aggregateFunction, aggVector, multiplicity,
            aggStateOffset);
    } else if (aggVector->state->isFlat()) {
        updateFlatUnFlatKeyFlatAggVectorState(flatKeyVectors, unFlatKeyVectors, aggregateFunction,
            aggVector, multiplicity, aggStateOffset);
    } else if (unFlatKeyVectors.empty()) {
        updateFlatKeyUnFlatAggVectorState(flatKeyVectors, aggregateFunction, aggVector,
            multiplicity, aggStateOffset);
    } else if (!unFlatKeyVectors.empty() && (aggVector->state == unFlatKeyVectors[0]->state)) {
        updateBothUnFlatSameDCAggVectorState(flatKeyVectors, unFlatKeyVectors, aggregateFunction,
            aggVector, multiplicity, aggStateOffset);
    } else {
        updateBothUnFlatDifferentDCAggVectorState(flatKeyVectors, unFlatKeyVectors,
            aggregateFunction, aggVector, multiplicity, aggStateOffset);
    }
}

void AggregateHashTable::updateAggStates(const std::vector<ValueVector*>& flatKeyVectors,
    const std::vector<ValueVector*>& unFlatKeyVectors,
    const std::vector<AggregateInput>& aggregateInputs, uint64_t resultSetMultiplicity) {
    auto aggregateStateOffset = aggStateColOffsetInFT;
    for (auto i = 0u; i < aggregateFunctions.size(); i++) {
        auto multiplicity = resultSetMultiplicity;
        for (auto& dataChunk : aggregateInputs[i].multiplicityChunks) {
            multiplicity *= dataChunk->state->getSelVector().getSelSize();
        }
        updateAggFuncs[i](this, flatKeyVectors, unFlatKeyVectors, aggregateFunctions[i],
            aggregateInputs[i].aggregateVector, multiplicity, i, aggregateStateOffset);
        aggregateStateOffset += aggregateFunctions[i]->getAggregateStateSize();
    }
}

void AggregateHashTable::fillEntryWithInitialNullAggregateState(uint8_t* entry) {
    for (auto i = 0u; i < aggregateFunctions.size(); i++) {
        factorizedTable->updateFlatCellNoNull(entry, aggStateColIdxInFT + i,
            (void*)aggregateFunctions[i]->getInitialNullAggregateState());
    }
}

void AggregateHashTable::fillHashSlot(hash_t hash, uint8_t* groupByKeysAndAggregateStateBuffer) {
    auto slotIdx = getSlotIdxForHash(hash);
    auto hashSlot = getHashSlot(slotIdx);
    while (true) {
        if (hashSlot->entry) {
            increaseSlotIdx(slotIdx);
            hashSlot = getHashSlot(slotIdx);
            continue;
        }
        break;
    }
    hashSlot->hash = hash;
    hashSlot->entry = groupByKeysAndAggregateStateBuffer;
}

void AggregateHashTable::addDataBlocksIfNecessary(uint64_t maxNumHashSlots) {
    auto numHashSlotsPerBlock = (uint64_t)1 << numSlotsPerBlockLog2;
    auto numHashSlotsBlocksNeeded =
        (maxNumHashSlots + numHashSlotsPerBlock - 1) / numHashSlotsPerBlock;
    while (hashSlotsBlocks.size() < numHashSlotsBlocksNeeded) {
        hashSlotsBlocks.emplace_back(std::make_unique<DataBlock>(&memoryManager, HASH_BLOCK_SIZE));
    }
}

void AggregateHashTable::resizeHashTableIfNecessary(uint32_t maxNumDistinctHashKeys) {
    if (factorizedTable->getNumTuples() + maxNumDistinctHashKeys > maxNumHashSlots ||
        (double)factorizedTable->getNumTuples() + maxNumDistinctHashKeys >
            (double)maxNumHashSlots / DEFAULT_HT_LOAD_FACTOR) {
        resize(maxNumHashSlots * 2);
    }
}

void AggregateHashTable::updateNullAggVectorState(const std::vector<ValueVector*>& flatKeyVectors,
    const std::vector<ValueVector*>& unFlatKeyVectors,
    std::unique_ptr<AggregateFunction>& aggregateFunction, uint64_t multiplicity,
    uint32_t aggStateOffset) {
    if (unFlatKeyVectors.empty()) {
        auto pos = flatKeyVectors[0]->state->getSelVector()[0];
        aggregateFunction->updatePosState(hashSlotsToUpdateAggState[pos]->entry + aggStateOffset,
            nullptr, multiplicity, 0 /* dummy pos */, &memoryManager);
    } else if (unFlatKeyVectors[0]->state->getSelVector().isUnfiltered()) {
        auto selectedSize = unFlatKeyVectors[0]->state->getSelVector().getSelSize();
        for (auto i = 0u; i < selectedSize; i++) {
            aggregateFunction->updatePosState(hashSlotsToUpdateAggState[i]->entry + aggStateOffset,
                nullptr, multiplicity, 0 /* dummy pos */, &memoryManager);
        }
    } else {
        auto selectedSize = unFlatKeyVectors[0]->state->getSelVector().getSelSize();
        for (auto i = 0u; i < selectedSize; i++) {
            auto pos = unFlatKeyVectors[0]->state->getSelVector()[i];
            aggregateFunction->updatePosState(hashSlotsToUpdateAggState[pos]->entry +
                                                  aggStateOffset,
                nullptr, multiplicity, 0 /* dummy pos */, &memoryManager);
        }
    }
}

void AggregateHashTable::updateBothFlatAggVectorState(
    const std::vector<ValueVector*>& /*flatKeyVectors*/,
    std::unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector,
    uint64_t multiplicity, uint32_t aggStateOffset) {
    auto aggPos = aggVector->state->getSelVector()[0];
    if (!aggVector->isNull(aggPos)) {
        aggregateFunction->updatePosState(
            hashSlotsToUpdateAggState[hashVector->state->getSelVector()[0]]->entry + aggStateOffset,
            aggVector, multiplicity, aggPos, &memoryManager);
    }
}

void AggregateHashTable::updateFlatUnFlatKeyFlatAggVectorState(
    const std::vector<ValueVector*>& /*flatKeyVectors*/,
    const std::vector<ValueVector*>& unFlatKeyVectors,
    std::unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector,
    uint64_t multiplicity, uint32_t aggStateOffset) {
    auto aggPos = aggVector->state->getSelVector()[0];
    auto selectedSize = unFlatKeyVectors[0]->state->getSelVector().getSelSize();
    if (!aggVector->isNull(aggPos)) {
        if (unFlatKeyVectors[0]->state->getSelVector().isUnfiltered()) {
            for (auto i = 0u; i < selectedSize; i++) {
                aggregateFunction->updatePosState(hashSlotsToUpdateAggState[i]->entry +
                                                      aggStateOffset,
                    aggVector, multiplicity, aggPos, &memoryManager);
            }
        } else {
            for (auto i = 0u; i < selectedSize; i++) {
                auto pos = unFlatKeyVectors[0]->state->getSelVector()[i];
                aggregateFunction->updatePosState(hashSlotsToUpdateAggState[pos]->entry +
                                                      aggStateOffset,
                    aggVector, multiplicity, aggPos, &memoryManager);
            }
        }
    }
}

void AggregateHashTable::updateFlatKeyUnFlatAggVectorState(
    const std::vector<ValueVector*>& flatKeyVectors,
    std::unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector,
    uint64_t multiplicity, uint32_t aggStateOffset) {
    auto groupByKeyPos = flatKeyVectors[0]->state->getSelVector()[0];
    auto aggVecSelectedSize = aggVector->state->getSelVector().getSelSize();
    if (aggVector->hasNoNullsGuarantee()) {
        if (aggVector->state->getSelVector().isUnfiltered()) {
            for (auto i = 0u; i < aggVecSelectedSize; i++) {
                aggregateFunction->updatePosState(hashSlotsToUpdateAggState[groupByKeyPos]->entry +
                                                      aggStateOffset,
                    aggVector, multiplicity, aggVector->state->getSelVector()[i], &memoryManager);
            }
        } else {
            for (auto i = 0u; i < aggVecSelectedSize; i++) {
                aggregateFunction->updatePosState(hashSlotsToUpdateAggState[groupByKeyPos]->entry +
                                                      aggStateOffset,
                    aggVector, multiplicity, aggVector->state->getSelVector()[i], &memoryManager);
            }
        }
    } else {
        if (aggVector->state->getSelVector().isUnfiltered()) {
            for (auto i = 0u; i < aggVecSelectedSize; i++) {
                if (!aggVector->isNull(i)) {
                    aggregateFunction->updatePosState(
                        hashSlotsToUpdateAggState[groupByKeyPos]->entry + aggStateOffset, aggVector,
                        multiplicity, i, &memoryManager);
                }
            }
        } else {
            for (auto i = 0u; i < aggVecSelectedSize; i++) {
                auto pos = aggVector->state->getSelVector()[i];
                if (!aggVector->isNull(pos)) {
                    aggregateFunction->updatePosState(
                        hashSlotsToUpdateAggState[groupByKeyPos]->entry + aggStateOffset, aggVector,
                        multiplicity, pos, &memoryManager);
                }
            }
        }
    }
}

void AggregateHashTable::updateBothUnFlatSameDCAggVectorState(
    const std::vector<ValueVector*>& /*flatKeyVectors*/,
    const std::vector<ValueVector*>& /*unFlatKeyVectors*/,
    std::unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector,
    uint64_t multiplicity, uint32_t aggStateOffset) {
    if (aggVector->hasNoNullsGuarantee()) {
        if (aggVector->state->getSelVector().isUnfiltered()) {
            for (auto i = 0u; i < aggVector->state->getSelVector().getSelSize(); i++) {
                aggregateFunction->updatePosState(hashSlotsToUpdateAggState[i]->entry +
                                                      aggStateOffset,
                    aggVector, multiplicity, i, &memoryManager);
            }
        } else {
            for (auto i = 0u; i < aggVector->state->getSelVector().getSelSize(); i++) {
                auto pos = aggVector->state->getSelVector()[i];
                aggregateFunction->updatePosState(hashSlotsToUpdateAggState[pos]->entry +
                                                      aggStateOffset,
                    aggVector, multiplicity, pos, &memoryManager);
            }
        }
    } else {
        if (aggVector->state->getSelVector().isUnfiltered()) {
            for (auto i = 0u; i < aggVector->state->getSelVector().getSelSize(); i++) {
                if (!aggVector->isNull(i)) {
                    aggregateFunction->updatePosState(hashSlotsToUpdateAggState[i]->entry +
                                                          aggStateOffset,
                        aggVector, multiplicity, i, &memoryManager);
                }
            }
        } else {
            for (auto i = 0u; i < aggVector->state->getSelVector().getSelSize(); i++) {
                auto pos = aggVector->state->getSelVector()[i];
                if (!aggVector->isNull(pos)) {
                    aggregateFunction->updatePosState(hashSlotsToUpdateAggState[pos]->entry +
                                                          aggStateOffset,
                        aggVector, multiplicity, pos, &memoryManager);
                }
            }
        }
    }
}

void AggregateHashTable::updateBothUnFlatDifferentDCAggVectorState(
    const std::vector<ValueVector*>& /*flatKeyVectors*/,
    const std::vector<ValueVector*>& unFlatKeyVectors,
    std::unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector,
    uint64_t multiplicity, uint32_t aggStateOffset) {
    auto selectedSize = unFlatKeyVectors[0]->state->getSelVector().getSelSize();
    if (unFlatKeyVectors[0]->state->getSelVector().isUnfiltered()) {
        for (auto i = 0u; i < selectedSize; i++) {
            aggregateFunction->updateAllState(hashSlotsToUpdateAggState[i]->entry + aggStateOffset,
                aggVector, multiplicity, &memoryManager);
        }
    } else {
        for (auto i = 0u; i < selectedSize; i++) {
            auto pos = unFlatKeyVectors[0]->state->getSelVector()[i];
            aggregateFunction->updateAllState(hashSlotsToUpdateAggState[pos]->entry +
                                                  aggStateOffset,
                aggVector, multiplicity, &memoryManager);
        }
    }
}

std::unique_ptr<AggregateHashTable> AggregateHashTableUtils::createDistinctHashTable(
    MemoryManager& memoryManager, const std::vector<LogicalType>& groupByKeyTypes,
    const LogicalType& distinctKeyType) {
    std::vector<LogicalType> hashKeyTypes(groupByKeyTypes.size() + 1);
    auto tableSchema = FactorizedTableSchema();
    auto i = 0u;
    // Group by key columns
    for (; i < groupByKeyTypes.size(); i++) {
        hashKeyTypes[i] = groupByKeyTypes[i].copy();
        auto size = LogicalTypeUtils::getRowLayoutSize(hashKeyTypes[i]);
        auto columnSchema = ColumnSchema(false /* isUnFlat */, 0 /* groupID */, size);
        tableSchema.appendColumn(std::move(columnSchema));
    }
    // Distinct key column
    hashKeyTypes[i] = distinctKeyType.copy();
    auto columnSchema = ColumnSchema(false /* isUnFlat */, 0 /* groupID */,
        LogicalTypeUtils::getRowLayoutSize(distinctKeyType));
    tableSchema.appendColumn(std::move(columnSchema));
    // Hash column
    tableSchema.appendColumn(ColumnSchema(false /* isUnFlat */, 0 /* groupID */, sizeof(hash_t)));
    return std::make_unique<AggregateHashTable>(memoryManager, std::move(hashKeyTypes),
        std::vector<LogicalType>{} /* empty payload types */, 0 /* numEntriesToAllocate */,
        std::move(tableSchema));
}

} // namespace processor
} // namespace kuzu
