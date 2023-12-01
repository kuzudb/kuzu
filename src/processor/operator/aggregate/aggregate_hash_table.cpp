#include "processor/operator/aggregate/aggregate_hash_table.h"

#include "common/null_buffer.h"
#include "common/utils.h"
#include "function/comparison/comparison_functions.h"
#include "function/hash/vector_hash_functions.h"

using namespace kuzu::common;
using namespace kuzu::function;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

AggregateHashTable::AggregateHashTable(MemoryManager& memoryManager,
    std::vector<LogicalType> keyDataTypes, std::vector<LogicalType> dependentKeyDataTypes,
    const std::vector<std::unique_ptr<AggregateFunction>>& aggregateFunctions,
    uint64_t numEntriesToAllocate)
    : BaseHashTable{memoryManager}, keyDataTypes{std::move(keyDataTypes)},
      dependentKeyDataTypes{std::move(dependentKeyDataTypes)} {
    initializeFT(aggregateFunctions);
    initializeHashTable(numEntriesToAllocate);
    distinctHashTables = AggregateHashTableUtils::createDistinctHashTables(
        memoryManager, this->keyDataTypes, this->aggregateFunctions);
    initializeTmpVectors();
}

void AggregateHashTable::append(const std::vector<ValueVector*>& flatKeyVectors,
    const std::vector<ValueVector*>& unFlatKeyVectors,
    const std::vector<ValueVector*>& dependentKeyVectors, common::DataChunkState* leadingState,
    const std::vector<std::unique_ptr<AggregateInput>>& aggregateInputs,
    uint64_t resultSetMultiplicity) {
    resizeHashTableIfNecessary(leadingState->selVector->selectedSize);
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
    if (groupByFlatKeyVectors.empty()) {
        VectorHashFunction::computeHash(aggregateVector, hashVector.get());
    } else {
        VectorHashFunction::computeHash(groupByFlatKeyVectors[0], hashVector.get());
        computeAndCombineVecHash(groupByFlatKeyVectors, 1 /* startVecIdx */);
        auto tmpHashResultVector =
            std::make_unique<ValueVector>(LogicalTypeID::INT64, &memoryManager);
        auto tmpHashCombineResultVector =
            std::make_unique<ValueVector>(LogicalTypeID::INT64, &memoryManager);
        VectorHashFunction::computeHash(aggregateVector, tmpHashResultVector.get());
        VectorHashFunction::combineHash(
            hashVector.get(), tmpHashResultVector.get(), tmpHashCombineResultVector.get());
        hashVector = std::move(tmpHashResultVector);
    }
    hash_t hash = hashVector->getValue<hash_t>(hashVector->state->selVector->selectedPositions[0]);
    auto distinctHTEntry = findEntryInDistinctHT(distinctKeyVectors, hash);
    if (distinctHTEntry == nullptr) {
        createEntryInDistinctHT(distinctKeyVectors, hash);
        return true;
    }
    return false;
}

void AggregateHashTable::merge(AggregateHashTable& other) {
    std::shared_ptr<DataChunkState> vectorsToScanState = std::make_shared<DataChunkState>();
    std::vector<ValueVector*> vectorsToScan(keyDataTypes.size() + dependentKeyDataTypes.size());
    std::vector<ValueVector*> groupByHashVectors(keyDataTypes.size());
    std::vector<ValueVector*> groupByNonHashVectors(dependentKeyDataTypes.size());
    std::vector<std::unique_ptr<ValueVector>> hashKeyVectors(keyDataTypes.size());
    std::vector<std::unique_ptr<ValueVector>> nonHashKeyVectors(groupByNonHashVectors.size());
    for (auto i = 0u; i < keyDataTypes.size(); i++) {
        auto hashKeyVec = std::make_unique<ValueVector>(keyDataTypes[i], &memoryManager);
        hashKeyVec->state = vectorsToScanState;
        vectorsToScan[i] = hashKeyVec.get();
        groupByHashVectors[i] = hashKeyVec.get();
        hashKeyVectors[i] = std::move(hashKeyVec);
    }
    for (auto i = 0u; i < dependentKeyDataTypes.size(); i++) {
        auto nonHashKeyVec =
            std::make_unique<ValueVector>(dependentKeyDataTypes[i], &memoryManager);
        nonHashKeyVec->state = vectorsToScanState;
        vectorsToScan[i + keyDataTypes.size()] = nonHashKeyVec.get();
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
        auto numTuplesToScan = std::min(
            other.factorizedTable->getNumTuples() - startTupleIdx, DEFAULT_VECTOR_CAPACITY);
        other.factorizedTable->scan(vectorsToScan, startTupleIdx, numTuplesToScan, colIdxesToScan);
        findHashSlots(std::vector<ValueVector*>(), groupByHashVectors, groupByNonHashVectors,
            vectorsToScanState.get());
        auto aggregateStateOffset = aggStateColOffsetInFT;
        for (auto& aggregateFunction : aggregateFunctions) {
            for (auto i = 0u; i < numTuplesToScan; i++) {
                aggregateFunction->combineState(
                    hashSlotsToUpdateAggState[i]->entry + aggregateStateOffset,
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
    const std::vector<std::unique_ptr<AggregateFunction>>& aggFuncs) {
    auto isUnflat = false;
    auto dataChunkPos = 0u;
    std::unique_ptr<FactorizedTableSchema> tableSchema = std::make_unique<FactorizedTableSchema>();
    aggStateColIdxInFT = keyDataTypes.size() + dependentKeyDataTypes.size();
    compareFuncs.resize(keyDataTypes.size());
    auto colIdx = 0u;
    for (auto& dataType : keyDataTypes) {
        auto size = LogicalTypeUtils::getRowLayoutSize(dataType);
        tableSchema->appendColumn(std::make_unique<ColumnSchema>(isUnflat, dataChunkPos, size));
        getCompareEntryWithKeysFunc(dataType, compareFuncs[colIdx]);
        numBytesForKeys += size;
        colIdx++;
    }
    for (auto& dataType : dependentKeyDataTypes) {
        auto size = LogicalTypeUtils::getRowLayoutSize(dataType);
        tableSchema->appendColumn(std::make_unique<ColumnSchema>(isUnflat, dataChunkPos, size));
        numBytesForDependentKeys += size;
        colIdx++;
    }
    aggStateColOffsetInFT = numBytesForKeys + numBytesForDependentKeys;

    aggregateFunctions.reserve(aggFuncs.size());
    updateAggFuncs.reserve(aggFuncs.size());
    for (auto i = 0u; i < aggFuncs.size(); i++) {
        auto& aggFunc = aggFuncs[i];
        tableSchema->appendColumn(std::make_unique<ColumnSchema>(
            isUnflat, dataChunkPos, aggFunc->getAggregateStateSize()));
        aggregateFunctions.push_back(aggFunc->clone());
        updateAggFuncs.push_back(aggFunc->isFunctionDistinct() ?
                                     &AggregateHashTable::updateDistinctAggState :
                                     &AggregateHashTable::updateAggState);
    }
    tableSchema->appendColumn(
        std::make_unique<ColumnSchema>(isUnflat, dataChunkPos, sizeof(hash_t)));
    hashColIdxInFT = aggStateColIdxInFT + aggFuncs.size();
    hashColOffsetInFT = tableSchema->getColOffset(hashColIdxInFT);
    factorizedTable = std::make_unique<FactorizedTable>(&memoryManager, std::move(tableSchema));
}

void AggregateHashTable::initializeHashTable(uint64_t numEntriesToAllocate) {
    setMaxNumHashSlots(nextPowerOfTwo(
        std::max(BufferPoolConstants::PAGE_256KB_SIZE / sizeof(HashSlot), numEntriesToAllocate)));
    auto numHashSlotsPerBlock = BufferPoolConstants::PAGE_256KB_SIZE / sizeof(HashSlot);
    initSlotConstant(numHashSlotsPerBlock);
    auto numDataBlocks =
        maxNumHashSlots / numHashSlotsPerBlock + (maxNumHashSlots % numHashSlotsPerBlock != 0);
    for (auto i = 0u; i < numDataBlocks; i++) {
        hashSlotsBlocks.emplace_back(std::make_unique<DataBlock>(&memoryManager));
    }
}

void AggregateHashTable::initializeTmpVectors() {
    hashState = std::make_shared<DataChunkState>();
    hashState->setToFlat();
    hashVector = std::make_unique<ValueVector>(LogicalTypeID::INT64, &memoryManager);
    hashVector->state = hashState;
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
        } else if ((slot->hash == hash) && matchFlatGroupByKeys(groupByKeyVectors, slot->entry)) {
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

void AggregateHashTable::initializeFTEntryWithFlatVec(
    ValueVector* flatVector, uint64_t numEntriesToInitialize, uint32_t colIdx) {
    KU_ASSERT(flatVector->state->isFlat());
    auto colOffset = factorizedTable->getTableSchema()->getColOffset(colIdx);
    auto pos = flatVector->state->selVector->selectedPositions[0];
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
            flatVector->copyToRowData(
                pos, entry + colOffset, factorizedTable->getInMemOverflowBuffer());
        }
    }
}

void AggregateHashTable::initializeFTEntryWithUnFlatVec(
    ValueVector* unFlatVector, uint64_t numEntriesToInitialize, uint32_t colIdx) {
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
            factorizedTable->updateFlatCell(
                hashSlotsToUpdateAggState[entryIdx]->entry, colIdx, unFlatVector, entryIdx);
        }
    }
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

uint8_t* AggregateHashTable::createEntryInDistinctHT(
    const std::vector<ValueVector*>& groupByHashKeyVectors, hash_t hash) {
    auto entry = factorizedTable->appendEmptyTuple();
    for (auto i = 0u; i < groupByHashKeyVectors.size(); i++) {
        factorizedTable->updateFlatCell(entry, i, groupByHashKeyVectors[i],
            groupByHashKeyVectors[i]->state->selVector->selectedPositions[0]);
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
    if (hashVector->state->selVector->isUnfiltered()) {
        for (auto i = 0u; i < hashVector->state->selVector->selectedSize; i++) {
            tmpValueIdxes[i] = i;
            tmpSlotIdxes[i] = getSlotIdxForHash(hashVector->getValue<hash_t>(i));
            hashSlotsToUpdateAggState[i] = getHashSlot(tmpSlotIdxes[i]);
        }
    } else {
        for (auto i = 0u; i < hashVector->state->selVector->selectedSize; i++) {
            auto pos = hashVector->state->selVector->selectedPositions[i];
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
    const std::vector<ValueVector*>& dependentKeyVectors, common::DataChunkState* leadingState) {
    initTmpHashSlotsAndIdxes();
    auto numEntriesToFindHashSlots = leadingState->selVector->selectedSize;
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
        initializeFTEntries(
            flatKeyVectors, unFlatKeyVectors, dependentKeyVectors, numFTEntriesToUpdate);
        numNoMatches =
            matchFTEntries(flatKeyVectors, unFlatKeyVectors, numMayMatches, numNoMatches);
        increaseHashSlotIdxes(numNoMatches);
        numEntriesToFindHashSlots = numNoMatches;
        memcpy(tmpValueIdxes.get(), noMatchIdxes.get(), DEFAULT_VECTOR_CAPACITY * sizeof(uint64_t));
    }
}

void AggregateHashTable::computeAndCombineVecHash(
    const std::vector<ValueVector*>& unFlatKeyVectors, uint32_t startVecIdx) {
    for (; startVecIdx < unFlatKeyVectors.size(); startVecIdx++) {
        auto keyVector = unFlatKeyVectors[startVecIdx];
        auto tmpHashResultVector =
            std::make_unique<ValueVector>(LogicalTypeID::INT64, &memoryManager);
        auto tmpHashCombineResultVector =
            std::make_unique<ValueVector>(LogicalTypeID::INT64, &memoryManager);
        VectorHashFunction::computeHash(keyVector, tmpHashResultVector.get());
        VectorHashFunction::combineHash(
            hashVector.get(), tmpHashResultVector.get(), tmpHashCombineResultVector.get());
        hashVector = std::move(tmpHashCombineResultVector);
    }
}

void AggregateHashTable::computeVectorHashes(const std::vector<ValueVector*>& flatKeyVectors,
    const std::vector<ValueVector*>& unFlatKeyVectors) {
    if (!flatKeyVectors.empty()) {
        VectorHashFunction::computeHash(flatKeyVectors[0], hashVector.get());
        computeAndCombineVecHash(flatKeyVectors, 1 /* startVecIdx */);
        computeAndCombineVecHash(unFlatKeyVectors, 0 /* startVecIdx */);
    } else {
        VectorHashFunction::computeHash(unFlatKeyVectors[0], hashVector.get());
        computeAndCombineVecHash(unFlatKeyVectors, 1 /* startVecIdx */);
    }
}

void AggregateHashTable::updateDistinctAggState(const std::vector<ValueVector*>& flatKeyVectors,
    const std::vector<ValueVector*>& /*unFlatKeyVectors*/,
    std::unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggregateVector,
    uint64_t /*multiplicity*/, uint32_t colIdx, uint32_t aggStateOffset) {
    auto distinctHT = distinctHashTables[colIdx].get();
    KU_ASSERT(distinctHT != nullptr);
    if (distinctHT->isAggregateValueDistinctForGroupByKeys(flatKeyVectors, aggregateVector)) {
        auto pos = aggregateVector->state->selVector->selectedPositions[0];
        if (!aggregateVector->isNull(pos)) {
            aggregateFunction->updatePosState(
                hashSlotsToUpdateAggState[flatKeyVectors.empty() ?
                                              0 :
                                              flatKeyVectors[0]
                                                  ->state->selVector->selectedPositions[0]]
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
        updateNullAggVectorState(
            flatKeyVectors, unFlatKeyVectors, aggregateFunction, multiplicity, aggStateOffset);
    } else if (aggVector->state->isFlat() && unFlatKeyVectors.empty()) {
        updateBothFlatAggVectorState(
            flatKeyVectors, aggregateFunction, aggVector, multiplicity, aggStateOffset);
    } else if (aggVector->state->isFlat()) {
        updateFlatUnFlatKeyFlatAggVectorState(flatKeyVectors, unFlatKeyVectors, aggregateFunction,
            aggVector, multiplicity, aggStateOffset);
    } else if (unFlatKeyVectors.empty()) {
        updateFlatKeyUnFlatAggVectorState(
            flatKeyVectors, aggregateFunction, aggVector, multiplicity, aggStateOffset);
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
    const std::vector<std::unique_ptr<AggregateInput>>& aggregateInputs,
    uint64_t resultSetMultiplicity) {
    auto aggregateStateOffset = aggStateColOffsetInFT;
    for (auto i = 0u; i < aggregateFunctions.size(); i++) {
        auto multiplicity = resultSetMultiplicity;
        for (auto& dataChunk : aggregateInputs[i]->multiplicityChunks) {
            multiplicity *= dataChunk->state->selVector->selectedSize;
        }
        updateAggFuncs[i](this, flatKeyVectors, unFlatKeyVectors, aggregateFunctions[i],
            aggregateInputs[i]->aggregateVector, multiplicity, i, aggregateStateOffset);
        aggregateStateOffset += aggregateFunctions[i]->getAggregateStateSize();
    }
}

bool AggregateHashTable::matchFlatGroupByKeys(
    const std::vector<ValueVector*>& keyVectors, uint8_t* entry) {
    for (auto i = 0u; i < keyVectors.size(); i++) {
        auto keyVector = keyVectors[i];
        KU_ASSERT(keyVector->state->isFlat());
        auto pos = keyVector->state->selVector->selectedPositions[0];
        auto isKeyVectorNull = keyVector->isNull(pos);
        auto isEntryKeyNull = factorizedTable->isNonOverflowColNull(
            entry + factorizedTable->getTableSchema()->getNullMapOffset(), i);
        // If either key or entry is null, we shouldn't compare the value of keyVector and
        // entry.
        if (isKeyVectorNull && isEntryKeyNull) {
            continue;
        } else if (isKeyVectorNull != isEntryKeyNull) {
            return false;
        }
        if (!compareFuncs[i](
                keyVector, pos, entry + factorizedTable->getTableSchema()->getColOffset(i))) {
            return false;
        }
    }
    return true;
}

uint64_t AggregateHashTable::matchUnFlatVecWithFTColumn(
    ValueVector* vector, uint64_t numMayMatches, uint64_t& numNoMatches, uint32_t colIdx) {
    KU_ASSERT(!vector->state->isFlat());
    auto colOffset = factorizedTable->getTableSchema()->getColOffset(colIdx);
    uint64_t mayMatchIdx = 0;
    if (vector->hasNoNullsGuarantee()) {
        if (factorizedTable->hasNoNullGuarantee(colIdx)) {
            for (auto i = 0u; i < numMayMatches; i++) {
                auto idx = mayMatchIdxes[i];
                if (compareFuncs[colIdx](
                        vector, idx, hashSlotsToUpdateAggState[idx]->entry + colOffset)) {
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
                if (compareFuncs[colIdx](
                        vector, idx, hashSlotsToUpdateAggState[idx]->entry + colOffset)) {
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

            if (compareFuncs[colIdx](
                    vector, idx, hashSlotsToUpdateAggState[idx]->entry + colOffset)) {
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
    KU_ASSERT(vector->state->isFlat());
    auto colOffset = factorizedTable->getTableSchema()->getColOffset(colIdx);
    uint64_t mayMatchIdx = 0;
    auto pos = vector->state->selVector->selectedPositions[0];
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
        if (compareFuncs[colIdx](vector, pos, hashSlotsToUpdateAggState[idx]->entry + colOffset)) {
            mayMatchIdxes[mayMatchIdx++] = idx;
        } else {
            noMatchIdxes[numNoMatches++] = idx;
        }
    }
    return mayMatchIdx;
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
        hashSlotsBlocks.emplace_back(std::make_unique<DataBlock>(&memoryManager));
    }
}

void AggregateHashTable::resizeHashTableIfNecessary(uint32_t maxNumDistinctHashKeys) {
    if (factorizedTable->getNumTuples() + maxNumDistinctHashKeys > maxNumHashSlots ||
        (double)factorizedTable->getNumTuples() + maxNumDistinctHashKeys >
            (double)maxNumHashSlots / DEFAULT_HT_LOAD_FACTOR) {
        resize(maxNumHashSlots * 2);
    }
}

template<typename T>
static bool compareEntry(common::ValueVector* vector, uint32_t vectorPos, const uint8_t* entry) {
    uint8_t result;
    auto key = vector->getData() + vectorPos * vector->getNumBytesPerValue();
    function::Equals::operation(
        *(T*)key, *(T*)entry, result, nullptr /* leftVector */, nullptr /* rightVector */);
    return result != 0;
}

static bool compareNodeEntry(
    common::ValueVector* vector, uint32_t vectorPos, const uint8_t* entry) {
    KU_ASSERT(0 == common::StructType::getFieldIdx(&vector->dataType, common::InternalKeyword::ID));
    auto idVector = common::StructVector::getFieldVector(vector, 0).get();
    return compareEntry<common::internalID_t>(idVector, vectorPos,
        entry + common::NullBuffer::getNumBytesForNullValues(
                    common::StructType::getNumFields(&vector->dataType)));
}

static bool compareRelEntry(common::ValueVector* vector, uint32_t vectorPos, const uint8_t* entry) {
    KU_ASSERT(3 == common::StructType::getFieldIdx(&vector->dataType, common::InternalKeyword::ID));
    auto idVector = common::StructVector::getFieldVector(vector, 3).get();
    return compareEntry<common::internalID_t>(idVector, vectorPos,
        entry + sizeof(common::internalID_t) * 2 + sizeof(common::ku_string_t) +
            common::NullBuffer::getNumBytesForNullValues(
                common::StructType::getNumFields(&vector->dataType)));
}

void AggregateHashTable::getCompareEntryWithKeysFunc(
    const LogicalType& logicalType, compare_function_t& func) {
    switch (logicalType.getPhysicalType()) {
    case PhysicalTypeID::INTERNAL_ID: {
        func = compareEntry<nodeID_t>;
        return;
    }
    case PhysicalTypeID::BOOL: {
        func = compareEntry<bool>;
        return;
    }
    case PhysicalTypeID::INT64: {
        func = compareEntry<int64_t>;
        return;
    }
    case PhysicalTypeID::INT32: {
        func = compareEntry<int32_t>;
        return;
    }
    case PhysicalTypeID::INT16: {
        func = compareEntry<int16_t>;
        return;
    }
    case PhysicalTypeID::INT8: {
        func = compareEntry<int8_t>;
        return;
    }
    case PhysicalTypeID::UINT64: {
        func = compareEntry<uint64_t>;
        return;
    }
    case PhysicalTypeID::UINT32: {
        func = compareEntry<uint32_t>;
        return;
    }
    case PhysicalTypeID::UINT16: {
        func = compareEntry<uint16_t>;
        return;
    }
    case PhysicalTypeID::UINT8: {
        func = compareEntry<uint8_t>;
        return;
    }
    case PhysicalTypeID::INT128: {
        func = compareEntry<int128_t>;
        return;
    }
    case PhysicalTypeID::DOUBLE: {
        func = compareEntry<double_t>;
        return;
    }
    case PhysicalTypeID::FLOAT: {
        func = compareEntry<float_t>;
        return;
    }
    case PhysicalTypeID::STRING: {
        func = compareEntry<ku_string_t>;
        return;
    }
    case PhysicalTypeID::INTERVAL: {
        func = compareEntry<interval_t>;
        return;
    }
    case PhysicalTypeID::STRUCT: {
        if (logicalType.getLogicalTypeID() == LogicalTypeID::NODE) {
            func = compareNodeEntry;
            return;
        } else if (logicalType.getLogicalTypeID() == LogicalTypeID::REL) {
            func = compareRelEntry;
            return;
        }
    }
    default: {
        throw RuntimeException(
            "Cannot compare data type " +
            PhysicalTypeUtils::physicalTypeToString(logicalType.getPhysicalType()));
    }
    }
}

void AggregateHashTable::updateNullAggVectorState(const std::vector<ValueVector*>& flatKeyVectors,
    const std::vector<ValueVector*>& unFlatKeyVectors,
    std::unique_ptr<AggregateFunction>& aggregateFunction, uint64_t multiplicity,
    uint32_t aggStateOffset) {
    if (unFlatKeyVectors.empty()) {
        auto pos = flatKeyVectors[0]->state->selVector->selectedPositions[0];
        aggregateFunction->updatePosState(hashSlotsToUpdateAggState[pos]->entry + aggStateOffset,
            nullptr, multiplicity, 0 /* dummy pos */, &memoryManager);
    } else if (unFlatKeyVectors[0]->state->selVector->isUnfiltered()) {
        auto selectedSize = unFlatKeyVectors[0]->state->selVector->selectedSize;
        for (auto i = 0u; i < selectedSize; i++) {
            aggregateFunction->updatePosState(hashSlotsToUpdateAggState[i]->entry + aggStateOffset,
                nullptr, multiplicity, 0 /* dummy pos */, &memoryManager);
        }
    } else {
        auto selectedSize = unFlatKeyVectors[0]->state->selVector->selectedSize;
        for (auto i = 0u; i < selectedSize; i++) {
            auto pos = unFlatKeyVectors[0]->state->selVector->selectedPositions[i];
            aggregateFunction->updatePosState(
                hashSlotsToUpdateAggState[pos]->entry + aggStateOffset, nullptr, multiplicity,
                0 /* dummy pos */, &memoryManager);
        }
    }
}

void AggregateHashTable::updateBothFlatAggVectorState(
    const std::vector<ValueVector*>& /*flatKeyVectors*/,
    std::unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector,
    uint64_t multiplicity, uint32_t aggStateOffset) {
    auto aggPos = aggVector->state->selVector->selectedPositions[0];
    if (!aggVector->isNull(aggPos)) {
        aggregateFunction->updatePosState(
            hashSlotsToUpdateAggState[hashVector->state->selVector->selectedPositions[0]]->entry +
                aggStateOffset,
            aggVector, multiplicity, aggPos, &memoryManager);
    }
}

void AggregateHashTable::updateFlatUnFlatKeyFlatAggVectorState(
    const std::vector<ValueVector*>& /*flatKeyVectors*/,
    const std::vector<ValueVector*>& unFlatKeyVectors,
    std::unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector,
    uint64_t multiplicity, uint32_t aggStateOffset) {
    auto aggPos = aggVector->state->selVector->selectedPositions[0];
    auto selectedSize = unFlatKeyVectors[0]->state->selVector->selectedSize;
    if (!aggVector->isNull(aggPos)) {
        if (unFlatKeyVectors[0]->state->selVector->isUnfiltered()) {
            for (auto i = 0u; i < selectedSize; i++) {
                aggregateFunction->updatePosState(
                    hashSlotsToUpdateAggState[i]->entry + aggStateOffset, aggVector, multiplicity,
                    aggPos, &memoryManager);
            }
        } else {
            for (auto i = 0u; i < selectedSize; i++) {
                auto pos = unFlatKeyVectors[0]->state->selVector->selectedPositions[i];
                aggregateFunction->updatePosState(
                    hashSlotsToUpdateAggState[pos]->entry + aggStateOffset, aggVector, multiplicity,
                    aggPos, &memoryManager);
            }
        }
    }
}

void AggregateHashTable::updateFlatKeyUnFlatAggVectorState(
    const std::vector<ValueVector*>& flatKeyVectors,
    std::unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector,
    uint64_t multiplicity, uint32_t aggStateOffset) {
    auto groupByKeyPos = flatKeyVectors[0]->state->selVector->selectedPositions[0];
    auto aggVecSelectedSize = aggVector->state->selVector->selectedSize;
    if (aggVector->hasNoNullsGuarantee()) {
        if (aggVector->state->selVector->isUnfiltered()) {
            for (auto i = 0u; i < aggVecSelectedSize; i++) {
                aggregateFunction->updatePosState(
                    hashSlotsToUpdateAggState[groupByKeyPos]->entry + aggStateOffset, aggVector,
                    multiplicity, aggVector->state->selVector->selectedPositions[i],
                    &memoryManager);
            }
        } else {
            for (auto i = 0u; i < aggVecSelectedSize; i++) {
                aggregateFunction->updatePosState(
                    hashSlotsToUpdateAggState[groupByKeyPos]->entry + aggStateOffset, aggVector,
                    multiplicity, aggVector->state->selVector->selectedPositions[i],
                    &memoryManager);
            }
        }
    } else {
        if (aggVector->state->selVector->isUnfiltered()) {
            for (auto i = 0u; i < aggVecSelectedSize; i++) {
                if (!aggVector->isNull(i)) {
                    aggregateFunction->updatePosState(
                        hashSlotsToUpdateAggState[0]->entry + aggStateOffset, aggVector,
                        multiplicity, i, &memoryManager);
                }
            }
        } else {
            for (auto i = 0u; i < aggVecSelectedSize; i++) {
                auto pos = aggVector->state->selVector->selectedPositions[i];
                if (!aggVector->isNull(pos)) {
                    aggregateFunction->updatePosState(
                        hashSlotsToUpdateAggState[0]->entry + aggStateOffset, aggVector,
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
        if (aggVector->state->selVector->isUnfiltered()) {
            for (auto i = 0u; i < aggVector->state->selVector->selectedSize; i++) {
                aggregateFunction->updatePosState(
                    hashSlotsToUpdateAggState[i]->entry + aggStateOffset, aggVector, multiplicity,
                    i, &memoryManager);
            }
        } else {
            for (auto i = 0u; i < aggVector->state->selVector->selectedSize; i++) {
                auto pos = aggVector->state->selVector->selectedPositions[i];
                aggregateFunction->updatePosState(
                    hashSlotsToUpdateAggState[pos]->entry + aggStateOffset, aggVector, multiplicity,
                    pos, &memoryManager);
            }
        }
    } else {
        if (aggVector->state->selVector->isUnfiltered()) {
            for (auto i = 0u; i < aggVector->state->selVector->selectedSize; i++) {
                if (!aggVector->isNull(i)) {
                    aggregateFunction->updatePosState(
                        hashSlotsToUpdateAggState[i]->entry + aggStateOffset, aggVector,
                        multiplicity, i, &memoryManager);
                }
            }
        } else {
            for (auto i = 0u; i < aggVector->state->selVector->selectedSize; i++) {
                auto pos = aggVector->state->selVector->selectedPositions[i];
                if (!aggVector->isNull(pos)) {
                    aggregateFunction->updatePosState(
                        hashSlotsToUpdateAggState[pos]->entry + aggStateOffset, aggVector,
                        multiplicity, pos, &memoryManager);
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
    auto selectedSize = unFlatKeyVectors[0]->state->selVector->selectedSize;
    if (unFlatKeyVectors[0]->state->selVector->isUnfiltered()) {
        for (auto i = 0u; i < selectedSize; i++) {
            aggregateFunction->updateAllState(hashSlotsToUpdateAggState[i]->entry + aggStateOffset,
                aggVector, multiplicity, &memoryManager);
        }
    } else {
        for (auto i = 0u; i < selectedSize; i++) {
            auto pos = unFlatKeyVectors[0]->state->selVector->selectedPositions[i];
            aggregateFunction->updateAllState(
                hashSlotsToUpdateAggState[pos]->entry + aggStateOffset, aggVector, multiplicity,
                &memoryManager);
        }
    }
}

std::vector<std::unique_ptr<AggregateHashTable>> AggregateHashTableUtils::createDistinctHashTables(
    MemoryManager& memoryManager, const std::vector<LogicalType>& groupByKeyDataTypes,
    const std::vector<std::unique_ptr<AggregateFunction>>& aggregateFunctions) {
    std::vector<std::unique_ptr<AggregateHashTable>> distinctHTs;
    for (auto& aggregateFunction : aggregateFunctions) {
        if (aggregateFunction->isFunctionDistinct()) {
            std::vector<LogicalType> distinctKeysDataTypes(groupByKeyDataTypes.size() + 1);
            for (auto i = 0u; i < groupByKeyDataTypes.size(); i++) {
                distinctKeysDataTypes[i] = groupByKeyDataTypes[i];
            }
            distinctKeysDataTypes[groupByKeyDataTypes.size()] =
                LogicalType{aggregateFunction->parameterTypeIDs[0]};
            std::vector<std::unique_ptr<AggregateFunction>> emptyFunctions;
            auto ht = std::make_unique<AggregateHashTable>(memoryManager,
                std::move(distinctKeysDataTypes), emptyFunctions, 0 /* numEntriesToAllocate */);
            distinctHTs.push_back(std::move(ht));
        } else {
            distinctHTs.push_back(nullptr);
        }
    }
    return distinctHTs;
}

} // namespace processor
} // namespace kuzu
