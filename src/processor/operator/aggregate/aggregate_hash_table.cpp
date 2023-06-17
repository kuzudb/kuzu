#include "processor/operator/aggregate/aggregate_hash_table.h"

#include "common/utils.h"
#include "function/aggregate/base_count.h"
#include "function/hash/vector_hash_operations.h"

using namespace kuzu::common;
using namespace kuzu::function;
using namespace kuzu::function::operation;
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

void AggregateHashTable::append(const std::vector<ValueVector*>& groupByFlatHashKeyVectors,
    const std::vector<ValueVector*>& groupByUnFlatHashKeyVectors,
    const std::vector<ValueVector*>& groupByDependentKeyVectors,
    const std::vector<std::unique_ptr<AggregateInput>>& aggregateInputs,
    uint64_t resultSetMultiplicity) {
    resizeHashTableIfNecessary(groupByUnFlatHashKeyVectors.empty() ?
                                   1 :
                                   groupByUnFlatHashKeyVectors[0]->state->selVector->selectedSize);
    computeVectorHashes(groupByFlatHashKeyVectors, groupByUnFlatHashKeyVectors);
    findHashSlots(
        groupByFlatHashKeyVectors, groupByUnFlatHashKeyVectors, groupByDependentKeyVectors);
    updateAggStates(groupByFlatHashKeyVectors, groupByUnFlatHashKeyVectors, aggregateInputs,
        resultSetMultiplicity);
}

bool AggregateHashTable::isAggregateValueDistinctForGroupByKeys(
    const std::vector<ValueVector*>& groupByFlatKeyVectors, ValueVector* aggregateVector) {
    std::vector<ValueVector*> distinctKeyVectors(groupByFlatKeyVectors.size() + 1);
    for (auto i = 0u; i < groupByFlatKeyVectors.size(); i++) {
        distinctKeyVectors[i] = groupByFlatKeyVectors[i];
    }
    distinctKeyVectors[groupByFlatKeyVectors.size()] = aggregateVector;
    if (groupByFlatKeyVectors.empty()) {
        VectorHashOperations::computeHash(aggregateVector, hashVector.get());
    } else {
        VectorHashOperations::computeHash(groupByFlatKeyVectors[0], hashVector.get());
        computeAndCombineVecHash(groupByFlatKeyVectors, 1 /* startVecIdx */);
        auto tmpHashResultVector =
            std::make_unique<ValueVector>(LogicalTypeID::INT64, &memoryManager);
        auto tmpHashCombineResultVector =
            std::make_unique<ValueVector>(LogicalTypeID::INT64, &memoryManager);
        VectorHashOperations::computeHash(aggregateVector, tmpHashResultVector.get());
        VectorHashOperations::combineHash(
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
        findHashSlots(std::vector<ValueVector*>(), groupByHashVectors, groupByNonHashVectors);
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
    compareFuncs.resize(aggStateColIdxInFT);
    auto colIdx = 0u;
    for (auto& dataType : keyDataTypes) {
        auto size = LogicalTypeUtils::getRowLayoutSize(dataType);
        tableSchema->appendColumn(std::make_unique<ColumnSchema>(isUnflat, dataChunkPos, size));
        hasStrCol = hasStrCol || dataType.getLogicalTypeID() == LogicalTypeID::STRING;
        getCompareEntryWithKeysFunc(dataType.getPhysicalType(), compareFuncs[colIdx]);
        numBytesForKeys += size;
        colIdx++;
    }
    for (auto& dataType : dependentKeyDataTypes) {
        auto size = LogicalTypeUtils::getRowLayoutSize(dataType);
        tableSchema->appendColumn(std::make_unique<ColumnSchema>(isUnflat, dataChunkPos, size));
        hasStrCol = hasStrCol || dataType.getLogicalTypeID() == LogicalTypeID::STRING;
        getCompareEntryWithKeysFunc(dataType.getPhysicalType(), compareFuncs[colIdx]);
        numBytesForDependentKeys += size;
        colIdx++;
    }
    aggStateColOffsetInFT = numBytesForKeys + numBytesForDependentKeys;

    aggregateFunctions.resize(aggFuncs.size());
    updateAggFuncs.resize(aggFuncs.size());
    for (auto i = 0u; i < aggFuncs.size(); i++) {
        auto& aggFunc = aggFuncs[i];
        tableSchema->appendColumn(std::make_unique<ColumnSchema>(
            isUnflat, dataChunkPos, aggFunc->getAggregateStateSize()));
        aggregateFunctions[i] = aggFunc->clone();
        updateAggFuncs[i] = aggFunc->isFunctionDistinct() ?
                                &AggregateHashTable::updateDistinctAggState :
                                &AggregateHashTable::updateAggState;
    }
    tableSchema->appendColumn(
        std::make_unique<ColumnSchema>(isUnflat, dataChunkPos, sizeof(hash_t)));
    hashColIdxInFT = aggStateColIdxInFT + aggFuncs.size();
    hashColOffsetInFT = tableSchema->getColOffset(hashColIdxInFT);
    factorizedTable = std::make_unique<FactorizedTable>(&memoryManager, std::move(tableSchema));
}

void AggregateHashTable::initializeHashTable(uint64_t numEntriesToAllocate) {
    maxNumHashSlots = nextPowerOfTwo(
        std::max(BufferPoolConstants::PAGE_256KB_SIZE / sizeof(HashSlot), numEntriesToAllocate));
    bitmask = maxNumHashSlots - 1;
    auto numHashSlotsPerBlock = BufferPoolConstants::PAGE_256KB_SIZE / sizeof(HashSlot);
    assert(numHashSlotsPerBlock == nextPowerOfTwo(numHashSlotsPerBlock));
    numSlotsPerBlockLog2 = log2(numHashSlotsPerBlock);
    slotIdxInBlockMask = BitmaskUtils::all1sMaskForLeastSignificantBits(numSlotsPerBlockLog2);
    auto numDataBlocks =
        maxNumHashSlots / numHashSlotsPerBlock + (maxNumHashSlots % numHashSlotsPerBlock != 0);
    for (auto i = 0u; i < numDataBlocks; i++) {
        hashSlotsBlocks.emplace_back(std::make_unique<DataBlock>(&memoryManager));
    }
}

void AggregateHashTable::initializeTmpVectors() {
    hashState = std::make_shared<DataChunkState>();
    hashState->currIdx = 0;
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
    maxNumHashSlots = newSize;
    bitmask = maxNumHashSlots - 1;
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
    ValueVector* groupByFlatVector, uint64_t numEntriesToInitialize, uint32_t colIdx) {
    assert(groupByFlatVector->state->isFlat());
    auto colOffset = factorizedTable->getTableSchema()->getColOffset(colIdx);
    if (groupByFlatVector->isNull(groupByFlatVector->state->selVector->selectedPositions[0])) {
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
            groupByFlatVector->copyToRowData(
                groupByFlatVector->state->selVector->selectedPositions[0], entry + colOffset,
                factorizedTable->getInMemOverflowBuffer());
        }
    }
}

void AggregateHashTable::initializeFTEntryWithUnflatVec(
    ValueVector* groupByUnflatVector, uint64_t numEntriesToInitialize, uint32_t colIdx) {
    assert(!groupByUnflatVector->state->isFlat());
    auto colOffset = factorizedTable->getTableSchema()->getColOffset(colIdx);
    if (groupByUnflatVector->hasNoNullsGuarantee()) {
        for (auto i = 0u; i < numEntriesToInitialize; i++) {
            auto entryIdx = entryIdxesToInitialize[i];
            groupByUnflatVector->copyToRowData(entryIdx,
                hashSlotsToUpdateAggState[entryIdx]->entry + colOffset,
                factorizedTable->getInMemOverflowBuffer());
        }
    } else {
        for (auto i = 0u; i < numEntriesToInitialize; i++) {
            auto entryIdx = entryIdxesToInitialize[i];
            factorizedTable->updateFlatCell(
                hashSlotsToUpdateAggState[entryIdx]->entry, colIdx, groupByUnflatVector, entryIdx);
        }
    }
}

void AggregateHashTable::initializeFTEntries(
    const std::vector<ValueVector*>& groupByFlatHashKeyVectors,
    const std::vector<ValueVector*>& groupByUnflatHashKeyVectors,
    const std::vector<ValueVector*>& groupByDependentKeyVectors,
    uint64_t numFTEntriesToInitialize) {
    auto colIdx = 0u;
    for (auto flatKeyVector : groupByFlatHashKeyVectors) {
        initializeFTEntryWithFlatVec(flatKeyVector, numFTEntriesToInitialize, colIdx++);
    }
    for (auto unflatKeyVector : groupByUnflatHashKeyVectors) {
        initializeFTEntryWithUnflatVec(unflatKeyVector, numFTEntriesToInitialize, colIdx++);
    }
    for (auto dependentKeyVector : groupByDependentKeyVectors) {
        if (dependentKeyVector->state->isFlat()) {
            initializeFTEntryWithFlatVec(dependentKeyVector, numFTEntriesToInitialize, colIdx++);
        } else {
            initializeFTEntryWithUnflatVec(dependentKeyVector, numFTEntriesToInitialize, colIdx++);
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
    if (hashVector->state->isFlat()) {
        auto pos = hashVector->state->selVector->selectedPositions[0];
        auto slotIdx = getSlotIdxForHash(hashVector->getValue<hash_t>(pos));
        tmpSlotIdxes[pos] = slotIdx;
        hashSlotsToUpdateAggState[pos] = getHashSlot(slotIdx);
        tmpValueIdxes[0] = pos;
    } else {
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
}

void AggregateHashTable::increaseHashSlotIdxes(uint64_t numNoMatches) {
    for (auto i = 0u; i < numNoMatches; i++) {
        auto idx = noMatchIdxes[i];
        increaseSlotIdx(tmpSlotIdxes[idx]);
        hashSlotsToUpdateAggState[idx] = getHashSlot(tmpSlotIdxes[idx]);
    }
}

void AggregateHashTable::findHashSlots(const std::vector<ValueVector*>& groupByFlatHashKeyVectors,
    const std::vector<ValueVector*>& groupByUnflatHashKeyVectors,
    const std::vector<ValueVector*>& groupByDependentKeyVectors) {
    initTmpHashSlotsAndIdxes();
    auto numEntriesToFindHashSlots =
        groupByUnflatHashKeyVectors.empty() ?
            1 :
            groupByUnflatHashKeyVectors[0]->state->selVector->selectedSize;
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
        initializeFTEntries(groupByFlatHashKeyVectors, groupByUnflatHashKeyVectors,
            groupByDependentKeyVectors, numFTEntriesToUpdate);
        numNoMatches = matchFTEntries(
            groupByFlatHashKeyVectors, groupByUnflatHashKeyVectors, numMayMatches, numNoMatches);
        increaseHashSlotIdxes(numNoMatches);
        numEntriesToFindHashSlots = numNoMatches;
        memcpy(tmpValueIdxes.get(), noMatchIdxes.get(), DEFAULT_VECTOR_CAPACITY * sizeof(uint64_t));
    }
}

void AggregateHashTable::computeAndCombineVecHash(
    const std::vector<ValueVector*>& groupByHashKeyVectors, uint32_t startVecIdx) {
    for (; startVecIdx < groupByHashKeyVectors.size(); startVecIdx++) {
        auto keyVector = groupByHashKeyVectors[startVecIdx];
        auto tmpHashResultVector =
            std::make_unique<ValueVector>(LogicalTypeID::INT64, &memoryManager);
        auto tmpHashCombineResultVector =
            std::make_unique<ValueVector>(LogicalTypeID::INT64, &memoryManager);
        VectorHashOperations::computeHash(keyVector, tmpHashResultVector.get());
        VectorHashOperations::combineHash(
            hashVector.get(), tmpHashResultVector.get(), tmpHashCombineResultVector.get());
        hashVector = std::move(tmpHashCombineResultVector);
    }
}

void AggregateHashTable::computeVectorHashes(
    const std::vector<ValueVector*>& groupByFlatHashKeyVectors,
    const std::vector<ValueVector*>& groupByUnflatHashKeyVectors) {
    if (!groupByFlatHashKeyVectors.empty()) {
        VectorHashOperations::computeHash(groupByFlatHashKeyVectors[0], hashVector.get());
        computeAndCombineVecHash(groupByFlatHashKeyVectors, 1 /* startVecIdx */);
        computeAndCombineVecHash(groupByUnflatHashKeyVectors, 0 /* startVecIdx */);
    } else {
        VectorHashOperations::computeHash(groupByUnflatHashKeyVectors[0], hashVector.get());
        computeAndCombineVecHash(groupByUnflatHashKeyVectors, 1 /* startVecIdx */);
    }
}

void AggregateHashTable::updateDistinctAggState(
    const std::vector<ValueVector*>& groupByFlatHashKeyVectors,
    const std::vector<ValueVector*>& groupByUnflatHashKeyVectors,
    std::unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggregateVector,
    uint64_t multiplicity, uint32_t colIdx, uint32_t aggStateOffset) {
    auto distinctHT = distinctHashTables[colIdx].get();
    assert(distinctHT != nullptr);
    if (distinctHT->isAggregateValueDistinctForGroupByKeys(
            groupByFlatHashKeyVectors, aggregateVector)) {
        auto pos = aggregateVector->state->selVector->selectedPositions[0];
        if (!aggregateVector->isNull(pos)) {
            aggregateFunction->updatePosState(
                hashSlotsToUpdateAggState[groupByFlatHashKeyVectors.empty() ?
                                              0 :
                                              groupByFlatHashKeyVectors[0]
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

void AggregateHashTable::updateAggState(const std::vector<ValueVector*>& groupByFlatHashKeyVectors,
    const std::vector<ValueVector*>& groupByUnflatHashKeyVectors,
    std::unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector,
    uint64_t multiplicity, uint32_t colIdx, uint32_t aggStateOffset) {
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

void AggregateHashTable::updateAggStates(const std::vector<ValueVector*>& groupByFlatHashKeyVectors,
    const std::vector<ValueVector*>& groupByUnFlatHashKeyVectors,
    const std::vector<std::unique_ptr<AggregateInput>>& aggregateInputs,
    uint64_t resultSetMultiplicity) {
    auto aggregateStateOffset = aggStateColOffsetInFT;
    for (auto i = 0u; i < aggregateFunctions.size(); i++) {
        auto multiplicity = resultSetMultiplicity;
        for (auto& dataChunk : aggregateInputs[i]->multiplicityChunks) {
            multiplicity *= dataChunk->state->selVector->selectedSize;
        }
        updateAggFuncs[i](this, groupByFlatHashKeyVectors, groupByUnFlatHashKeyVectors,
            aggregateFunctions[i], aggregateInputs[i]->aggregateVector, multiplicity, i,
            aggregateStateOffset);
        aggregateStateOffset += aggregateFunctions[i]->getAggregateStateSize();
    }
}

bool AggregateHashTable::matchFlatGroupByKeys(
    const std::vector<ValueVector*>& keyVectors, uint8_t* entry) {
    for (auto i = 0u; i < keyVectors.size(); i++) {
        auto keyVector = keyVectors[i];
        assert(keyVector->state->isFlat());
        auto pos = keyVector->state->selVector->selectedPositions[0];
        auto keyValue = keyVector->getData() + pos * keyVector->getNumBytesPerValue();
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
                keyValue, entry + factorizedTable->getTableSchema()->getColOffset(i))) {
            return false;
        }
    }
    return true;
}

uint64_t AggregateHashTable::matchUnflatVecWithFTColumn(
    ValueVector* vector, uint64_t numMayMatches, uint64_t& numNoMatches, uint32_t colIdx) {
    assert(!vector->state->isFlat());
    auto colOffset = factorizedTable->getTableSchema()->getColOffset(colIdx);
    uint64_t mayMatchIdx = 0;
    if (vector->hasNoNullsGuarantee()) {
        if (factorizedTable->hasNoNullGuarantee(colIdx)) {
            for (auto i = 0u; i < numMayMatches; i++) {
                auto idx = mayMatchIdxes[i];
                if (compareFuncs[colIdx](vector->getData() + idx * vector->getNumBytesPerValue(),
                        hashSlotsToUpdateAggState[idx]->entry + colOffset)) {
                    mayMatchIdxes[mayMatchIdx++] = idx;
                } else {
                    noMatchIdxes[numNoMatches++] = idx;
                }
            }
        } else {
            for (auto i = 0u; i < numMayMatches; i++) {
                auto idx = mayMatchIdxes[i];
                auto value = vector->getData() + idx * vector->getNumBytesPerValue();
                auto isEntryKeyNull = factorizedTable->isNonOverflowColNull(
                    hashSlotsToUpdateAggState[idx]->entry +
                        factorizedTable->getTableSchema()->getNullMapOffset(),
                    colIdx);
                if (isEntryKeyNull) {
                    noMatchIdxes[numNoMatches++] = idx;
                    continue;
                }
                if (compareFuncs[colIdx](
                        value, hashSlotsToUpdateAggState[idx]->entry + colOffset)) {
                    mayMatchIdxes[mayMatchIdx++] = idx;
                } else {
                    noMatchIdxes[numNoMatches++] = idx;
                }
            }
        }
    } else {
        for (auto i = 0u; i < numMayMatches; i++) {
            auto idx = mayMatchIdxes[i];
            auto value = vector->getData() + idx * vector->getNumBytesPerValue();
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
    auto colOffset = factorizedTable->getTableSchema()->getColOffset(colIdx);
    uint64_t mayMatchIdx = 0;
    auto pos = vector->state->selVector->selectedPositions[0];
    auto isVectorNull = vector->isNull(pos);
    auto value = vector->getData() + pos * vector->getNumBytesPerValue();
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
        if (compareFuncs[colIdx](value, hashSlotsToUpdateAggState[idx]->entry + colOffset)) {
            mayMatchIdxes[mayMatchIdx++] = idx;
        } else {
            noMatchIdxes[numNoMatches++] = idx;
        }
    }
    return mayMatchIdx;
}

uint64_t AggregateHashTable::matchFTEntries(
    const std::vector<ValueVector*>& groupByFlatHashKeyVectors,
    const std::vector<ValueVector*>& groupByUnflatHashKeyVectors, uint64_t numMayMatches,
    uint64_t numNoMatches) {
    auto colIdx = 0u;
    for (auto& groupByFlatHashKeyVector : groupByFlatHashKeyVectors) {
        numMayMatches = matchFlatVecWithFTColumn(
            groupByFlatHashKeyVector, numMayMatches, numNoMatches, colIdx++);
    }
    for (auto& groupByUnflatHashKeyVector : groupByUnflatHashKeyVectors) {
        numMayMatches = matchUnflatVecWithFTColumn(
            groupByUnflatHashKeyVector, numMayMatches, numNoMatches, colIdx++);
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

void AggregateHashTable::getCompareEntryWithKeysFunc(
    PhysicalTypeID physicalType, compare_function_t& func) {
    switch (physicalType) {
    case PhysicalTypeID::INTERNAL_ID: {
        func = compareEntryWithKeys<nodeID_t>;
        return;
    }
    case PhysicalTypeID::BOOL: {
        func = compareEntryWithKeys<bool>;
        return;
    }
    case PhysicalTypeID::INT64: {
        func = compareEntryWithKeys<int64_t>;
        return;
    }
    case PhysicalTypeID::INT32: {
        func = compareEntryWithKeys<int32_t>;
        return;
    }
    case PhysicalTypeID::INT16: {
        func = compareEntryWithKeys<int16_t>;
        return;
    }
    case PhysicalTypeID::DOUBLE: {
        func = compareEntryWithKeys<double_t>;
        return;
    }
    case PhysicalTypeID::FLOAT: {
        func = compareEntryWithKeys<float_t>;
        return;
    }
    case PhysicalTypeID::STRING: {
        func = compareEntryWithKeys<ku_string_t>;
        return;
    }
    case PhysicalTypeID::INTERVAL: {
        func = compareEntryWithKeys<interval_t>;
        return;
    }
    default: {
        throw RuntimeException(
            "Cannot compare data type " + PhysicalTypeUtils::physicalTypeToString(physicalType));
    }
    }
}

void AggregateHashTable::updateNullAggVectorState(
    const std::vector<ValueVector*>& groupByFlatHashKeyVectors,
    const std::vector<ValueVector*>& groupByUnflatHashKeyVectors,
    std::unique_ptr<AggregateFunction>& aggregateFunction, uint64_t multiplicity,
    uint32_t aggStateOffset) {
    if (groupByUnflatHashKeyVectors.empty()) {
        auto pos = groupByFlatHashKeyVectors[0]->state->selVector->selectedPositions[0];
        aggregateFunction->updatePosState(hashSlotsToUpdateAggState[pos]->entry + aggStateOffset,
            nullptr, multiplicity, 0 /* dummy pos */, &memoryManager);
    } else if (groupByUnflatHashKeyVectors[0]->state->selVector->isUnfiltered()) {
        auto selectedSize = groupByUnflatHashKeyVectors[0]->state->selVector->selectedSize;
        for (auto i = 0u; i < selectedSize; i++) {
            aggregateFunction->updatePosState(hashSlotsToUpdateAggState[i]->entry + aggStateOffset,
                nullptr, multiplicity, 0 /* dummy pos */, &memoryManager);
        }
    } else {
        auto selectedSize = groupByUnflatHashKeyVectors[0]->state->selVector->selectedSize;
        for (auto i = 0u; i < selectedSize; i++) {
            auto pos = groupByUnflatHashKeyVectors[0]->state->selVector->selectedPositions[i];
            aggregateFunction->updatePosState(
                hashSlotsToUpdateAggState[pos]->entry + aggStateOffset, nullptr, multiplicity,
                0 /* dummy pos */, &memoryManager);
        }
    }
}

void AggregateHashTable::updateBothFlatAggVectorState(
    const std::vector<ValueVector*>& groupByFlatHashKeyVectors,
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

void AggregateHashTable::updateFlatUnflatKeyFlatAggVectorState(
    const std::vector<ValueVector*>& groupByFlatHashKeyVectors,
    const std::vector<ValueVector*>& groupByUnflatHashKeyVectors,
    std::unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector,
    uint64_t multiplicity, uint32_t aggStateOffset) {
    auto aggPos = aggVector->state->selVector->selectedPositions[0];
    auto selectedSize = groupByUnflatHashKeyVectors[0]->state->selVector->selectedSize;
    if (!aggVector->isNull(aggPos)) {
        if (groupByUnflatHashKeyVectors[0]->state->selVector->isUnfiltered()) {
            for (auto i = 0u; i < selectedSize; i++) {
                aggregateFunction->updatePosState(
                    hashSlotsToUpdateAggState[i]->entry + aggStateOffset, aggVector, multiplicity,
                    aggPos, &memoryManager);
            }
        } else {
            for (auto i = 0u; i < selectedSize; i++) {
                auto pos = groupByUnflatHashKeyVectors[0]->state->selVector->selectedPositions[i];
                aggregateFunction->updatePosState(
                    hashSlotsToUpdateAggState[pos]->entry + aggStateOffset, aggVector, multiplicity,
                    aggPos, &memoryManager);
            }
        }
    }
}

void AggregateHashTable::updateFlatKeyUnflatAggVectorState(
    const std::vector<ValueVector*>& groupByFlatHashKeyVectors,
    std::unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector,
    uint64_t multiplicity, uint32_t aggStateOffset) {
    auto groupByKeyPos = groupByFlatHashKeyVectors[0]->state->selVector->selectedPositions[0];
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

void AggregateHashTable::updateBothUnflatSameDCAggVectorState(
    const std::vector<ValueVector*>& groupByFlatHashKeyVectors,
    const std::vector<ValueVector*>& groupByUnflatHashKeyVectors,
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

void AggregateHashTable::updateBothUnflatDifferentDCAggVectorState(
    const std::vector<ValueVector*>& groupByFlatHashKeyVectors,
    const std::vector<ValueVector*>& groupByUnflatHashKeyVectors,
    std::unique_ptr<AggregateFunction>& aggregateFunction, ValueVector* aggVector,
    uint64_t multiplicity, uint32_t aggStateOffset) {
    auto selectedSize = groupByUnflatHashKeyVectors[0]->state->selVector->selectedSize;
    if (groupByUnflatHashKeyVectors[0]->state->selVector->isUnfiltered()) {
        for (auto i = 0u; i < selectedSize; i++) {
            aggregateFunction->updateAllState(hashSlotsToUpdateAggState[i]->entry + aggStateOffset,
                aggVector, multiplicity, &memoryManager);
        }
    } else {
        for (auto i = 0u; i < selectedSize; i++) {
            auto pos = groupByUnflatHashKeyVectors[0]->state->selVector->selectedPositions[i];
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
                aggregateFunction->getInputDataType();
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
