#include "processor/operator/aggregate/aggregate_hash_table.h"

#include <cstdint>

#include "common/constants.h"
#include "common/utils.h"
#include "common/vector/value_vector.h"
#include "processor/operator/aggregate/hash_aggregate.h"
#include "processor/result/factorized_table.h"
#include "processor/result/factorized_table_schema.h"

using namespace kuzu::common;
using namespace kuzu::function;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

AggregateHashTable::AggregateHashTable(MemoryManager& memoryManager,
    std::vector<LogicalType> keyTypes, std::vector<LogicalType> payloadTypes,
    const std::vector<AggregateFunction>& aggregateFunctions,
    const std::vector<LogicalType>& distinctAggKeyTypes, uint64_t numEntriesToAllocate,
    FactorizedTableSchema tableSchema)
    : BaseHashTable{memoryManager, std::move(keyTypes)}, payloadTypes{std::move(payloadTypes)} {
    initializeFT(aggregateFunctions, std::move(tableSchema));
    initializeHashTable(numEntriesToAllocate);
    KU_ASSERT(aggregateFunctions.size() == distinctAggKeyTypes.size());
    for (auto i = 0u; i < this->aggregateFunctions.size(); ++i) {
        std::unique_ptr<AggregateHashTable> distinctHT;
        if (this->aggregateFunctions[i].isDistinct) {
            distinctHT = AggregateHashTableUtils::createDistinctHashTable(memoryManager,
                this->keyTypes, distinctAggKeyTypes[i]);
        } else {
            distinctHT = nullptr;
        }
        distinctHashTables.push_back(std::move(distinctHT));
    }
    initializeTmpVectors();
}

uint64_t AggregateHashTable::append(const std::vector<ValueVector*>& flatKeyVectors,
    const std::vector<ValueVector*>& unFlatKeyVectors,
    const std::vector<ValueVector*>& dependentKeyVectors, DataChunkState* leadingState,
    const std::vector<AggregateInput>& aggregateInputs, uint64_t resultSetMultiplicity) {
    const auto numFlatTuples = leadingState->getSelVector().getSelSize();
    resizeHashTableIfNecessary(numFlatTuples);
    computeVectorHashes(flatKeyVectors, unFlatKeyVectors);
    findHashSlots(flatKeyVectors, unFlatKeyVectors, dependentKeyVectors, leadingState);
    updateAggStates(flatKeyVectors, unFlatKeyVectors, aggregateInputs, resultSetMultiplicity);
    return numFlatTuples;
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

hash_t getHash(const FactorizedTable& table, ft_tuple_idx_t tupleIdx) {
    return *(hash_t*)(table.getTuple(tupleIdx) + table.getTableSchema()->getColOffset(
                                                     table.getTableSchema()->getNumColumns() - 1));
}

void AggregateHashTable::merge(FactorizedTable&& table) {
    KU_ASSERT(*table.getTableSchema() == *getTableSchema());
    resizeHashTableIfNecessary(table.getNumTuples());

    uint64_t startTupleIdx = 0;
    while (startTupleIdx < table.getNumTuples()) {
        auto numTuplesToScan =
            std::min(table.getNumTuples() - startTupleIdx, DEFAULT_VECTOR_CAPACITY);
        findHashSlots(table, startTupleIdx, numTuplesToScan);
        auto aggregateStateOffset = aggStateColOffsetInFT;
        for (auto& aggregateFunction : aggregateFunctions) {
            for (auto i = 0u; i < numTuplesToScan; i++) {
                aggregateFunction.combineState(hashSlotsToUpdateAggState[i]->entry +
                                                   aggregateStateOffset,
                    table.getTuple(startTupleIdx + i) + aggregateStateOffset, memoryManager);
            }
            aggregateStateOffset += aggregateFunction.getAggregateStateSize();
        }
        startTupleIdx += numTuplesToScan;
    }
}

void AggregateHashTable::finalizeAggregateStates() {
    for (auto i = 0u; i < getNumEntries(); ++i) {
        auto entry = factorizedTable->getTuple(i);
        auto aggregateStatesOffset = aggStateColOffsetInFT;
        for (auto& aggregateFunction : aggregateFunctions) {
            aggregateFunction.finalizeState(entry + aggregateStatesOffset);
            aggregateStatesOffset += aggregateFunction.getAggregateStateSize();
        }
    }
}

void AggregateHashTable::initializeFT(const std::vector<AggregateFunction>& aggFuncs,
    FactorizedTableSchema&& tableSchema) {
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
        aggregateFunctions.push_back(aggFunc.copy());
        updateAggFuncs.push_back(aggFunc.isFunctionDistinct() ?
                                     &AggregateHashTable::updateDistinctAggState :
                                     &AggregateHashTable::updateAggState);
    }
    hashColIdxInFT = tableSchema.getNumColumns() - 1;
    hashColOffsetInFT = tableSchema.getColOffset(hashColIdxInFT);
    factorizedTable = std::make_unique<FactorizedTable>(memoryManager, std::move(tableSchema));
}

void AggregateHashTable::initializeHashTable(uint64_t numEntriesToAllocate) {
    auto numHashSlotsPerBlock = prevPowerOfTwo(HASH_BLOCK_SIZE / sizeof(HashSlot));
    setMaxNumHashSlots(nextPowerOfTwo(std::max(numHashSlotsPerBlock, numEntriesToAllocate)));
    initSlotConstant(numHashSlotsPerBlock);
    auto numDataBlocks =
        maxNumHashSlots / numHashSlotsPerBlock + (maxNumHashSlots % numHashSlotsPerBlock != 0);
    for (auto i = 0u; i < numDataBlocks; i++) {
        hashSlotsBlocks.emplace_back(std::make_unique<DataBlock>(memoryManager, HASH_BLOCK_SIZE));
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
    factorizedTable->forEach(
        [&](auto tuple) { fillHashSlot(*(hash_t*)(tuple + hashColOffsetInFT), tuple); });
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

uint64_t AggregateHashTable::matchFTEntries(const FactorizedTable& srcTable, uint64_t startOffset,
    uint64_t numMayMatches, uint64_t numNoMatches) {
    for (auto colIdx = 0u; colIdx < keyTypes.size(); colIdx++) {
        auto colOffset = getTableSchema()->getColOffset(colIdx);
        uint64_t mayMatchIdx = 0;
        for (auto i = 0u; i < numMayMatches; i++) {
            auto idx = mayMatchIdxes[i];
            auto& slot = *hashSlotsToUpdateAggState[idx];
            auto isEntryKeyNull = factorizedTable->isNonOverflowColNull(
                slot.entry + getTableSchema()->getNullMapOffset(), colIdx);
            auto isSrcEntryKeyNull = srcTable.isNonOverflowColNull(startOffset + idx, colIdx);
            if (isEntryKeyNull && isSrcEntryKeyNull) {
                mayMatchIdxes[mayMatchIdx++] = idx;
                continue;
            } else if (isEntryKeyNull != isSrcEntryKeyNull) {
                noMatchIdxes[numNoMatches++] = idx;
                continue;
            }
            if (rawCompareEntryFuncs[colIdx](srcTable.getTuple(startOffset + idx) + colOffset,
                    hashSlotsToUpdateAggState[idx]->entry + colOffset)) {
                mayMatchIdxes[mayMatchIdx++] = idx;
            } else {
                noMatchIdxes[numNoMatches++] = idx;
            }
        }
        numMayMatches = mayMatchIdx;
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
        auto& slot = *hashSlotsToUpdateAggState[entryIdx];
        fillEntryWithInitialNullAggregateState(*factorizedTable, slot.entry);
        // Fill the hashValue in the ftEntry.
        factorizedTable->updateFlatCellNoNull(slot.entry, hashColIdxInFT,
            hashVector->getData() + hashVector->getNumBytesPerValue() * entryIdx);
    }
}

void AggregateHashTable::initializeFTEntries(const FactorizedTable& sourceTable,
    uint64_t sourceStartOffset, uint64_t numFTEntriesToInitialize) {
    // auto colIdx = 0u;
    for (size_t i = 0; i < numFTEntriesToInitialize; i++) {
        auto idx = entryIdxesToInitialize[i];
        auto& slot = *hashSlotsToUpdateAggState[idx];
        auto sourcePos = sourceStartOffset + idx;
        memcpy(slot.entry, sourceTable.getTuple(sourcePos),
            getTableSchema()->getNumBytesPerTuple());
    }
    for (auto i = 0u; i < numFTEntriesToInitialize; i++) {
        auto entryIdx = entryIdxesToInitialize[i];
        auto& slot = *hashSlotsToUpdateAggState[entryIdx];
        fillEntryWithInitialNullAggregateState(*factorizedTable, slot.entry);
        // Fill the hashValue in the ftEntry.
        factorizedTable->updateFlatCellNoNull(slot.entry, hashColIdxInFT,
            hashVector->getData() + hashVector->getNumBytesPerValue() * entryIdx);
    }
}

uint64_t AggregateHashTable::matchUnFlatVecWithFTColumn(ValueVector* vector, uint64_t numMayMatches,
    uint64_t& numNoMatches, uint32_t colIdx) {
    KU_ASSERT(!vector->state->isFlat());
    auto& schema = *getTableSchema();
    auto colOffset = schema.getColOffset(colIdx);
    uint64_t mayMatchIdx = 0;
    if (vector->hasNoNullsGuarantee()) {
        for (auto i = 0u; i < numMayMatches; i++) {
            auto idx = mayMatchIdxes[i];
            auto& slot = *hashSlotsToUpdateAggState[idx];
            auto isEntryKeyNull = factorizedTable->isNonOverflowColNull(
                slot.entry + schema.getNullMapOffset(), colIdx);
            if (isEntryKeyNull) {
                noMatchIdxes[numNoMatches++] = idx;
                continue;
            }
            if (compareEntryFuncs[colIdx](vector, idx, slot.entry + colOffset)) {
                mayMatchIdxes[mayMatchIdx++] = idx;
            } else {
                noMatchIdxes[numNoMatches++] = idx;
            }
        }
    } else {
        for (auto i = 0u; i < numMayMatches; i++) {
            auto idx = mayMatchIdxes[i];
            auto isKeyVectorNull = vector->isNull(idx);
            auto& slot = *hashSlotsToUpdateAggState[idx];
            auto isEntryKeyNull = factorizedTable->isNonOverflowColNull(
                slot.entry + schema.getNullMapOffset(), colIdx);
            if (isKeyVectorNull && isEntryKeyNull) {
                mayMatchIdxes[mayMatchIdx++] = idx;
                continue;
            } else if (isKeyVectorNull != isEntryKeyNull) {
                noMatchIdxes[numNoMatches++] = idx;
                continue;
            }

            if (compareEntryFuncs[colIdx](vector, idx, slot.entry + colOffset)) {
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
    auto colOffset = getTableSchema()->getColOffset(colIdx);
    uint64_t mayMatchIdx = 0;
    auto pos = vector->state->getSelVector()[0];
    auto isVectorNull = vector->isNull(pos);
    for (auto i = 0u; i < numMayMatches; i++) {
        auto idx = mayMatchIdxes[i];
        auto& slot = *hashSlotsToUpdateAggState[idx];
        auto isEntryKeyNull = factorizedTable->isNonOverflowColNull(
            slot.entry + getTableSchema()->getNullMapOffset(), colIdx);
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
    auto colOffset = getTableSchema()->getColOffset(colIdx);
    auto pos = flatVector->state->getSelVector()[0];
    if (flatVector->isNull(pos)) {
        for (auto i = 0u; i < numEntriesToInitialize; i++) {
            auto idx = entryIdxesToInitialize[i];
            auto& slot = *hashSlotsToUpdateAggState[idx];
            factorizedTable->setNonOverflowColNull(
                slot.entry + getTableSchema()->getNullMapOffset(), colIdx);
        }
    } else {
        for (auto i = 0u; i < numEntriesToInitialize; i++) {
            auto idx = entryIdxesToInitialize[i];
            auto& slot = *hashSlotsToUpdateAggState[idx];
            flatVector->copyToRowData(pos, slot.entry + colOffset,
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
            auto& slot = *hashSlotsToUpdateAggState[entryIdx];
            unFlatVector->copyToRowData(entryIdx, slot.entry + colOffset,
                factorizedTable->getInMemOverflowBuffer());
        }
    } else {
        for (auto i = 0u; i < numEntriesToInitialize; i++) {
            auto entryIdx = entryIdxesToInitialize[i];
            auto& slot = *hashSlotsToUpdateAggState[entryIdx];
            factorizedTable->updateFlatCell(slot.entry, colIdx, unFlatVector, entryIdx);
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
    fillEntryWithInitialNullAggregateState(*factorizedTable, entry);
    fillHashSlot(hash, entry);
    return entry;
}

void AggregateHashTable::increaseSlotIdx(uint64_t& slotIdx) const {
    slotIdx++;
    if (slotIdx >= maxNumHashSlots) {
        slotIdx = 0;
    }
}

void AggregateHashTable::initTmpHashSlotsAndIdxes(const FactorizedTable& sourceTable,
    uint64_t startOffset, uint64_t numTuples) {
    for (size_t i = 0; i < numTuples; i++) {
        tmpValueIdxes[i] = i;
        auto hash = getHash(sourceTable, startOffset + i);
        hashVector->setValue(i, hash);
        tmpSlotIdxes[i] = getSlotIdxForHash(hash);
        hashSlotsToUpdateAggState[i] = getHashSlot(tmpSlotIdxes[i]);
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
    KU_ASSERT(getNumEntries() + numEntriesToFindHashSlots < maxNumHashSlots);
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
        KU_ASSERT(numNoMatches <= numEntriesToFindHashSlots);
        numEntriesToFindHashSlots = numNoMatches;
        memcpy(tmpValueIdxes.get(), noMatchIdxes.get(), numNoMatches * sizeof(uint64_t));
    }
}

void AggregateHashTable::findHashSlots(const FactorizedTable& srcTable, uint64_t startOffset,
    uint64_t numEntriesToFindHashSlots) {
    initTmpHashSlotsAndIdxes(srcTable, startOffset, numEntriesToFindHashSlots);
    KU_ASSERT(getNumEntries() + numEntriesToFindHashSlots < maxNumHashSlots);
    while (numEntriesToFindHashSlots > 0) {
        uint64_t numFTEntriesToUpdate = 0;
        uint64_t numMayMatches = 0;
        uint64_t numNoMatches = 0;
        for (auto i = 0u; i < numEntriesToFindHashSlots; i++) {
            auto idx = tmpValueIdxes[i];
            auto slot = hashSlotsToUpdateAggState[idx];
            auto hash = hashVector->getValue<hash_t>(idx);
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
        initializeFTEntries(srcTable, startOffset, numFTEntriesToUpdate);
        numNoMatches = matchFTEntries(srcTable, startOffset, numMayMatches, numNoMatches);
        increaseHashSlotIdxes(numNoMatches);
        KU_ASSERT(numNoMatches <= numEntriesToFindHashSlots);
        numEntriesToFindHashSlots = numNoMatches;
        memcpy(tmpValueIdxes.get(), noMatchIdxes.get(), numNoMatches * sizeof(uint64_t));
    }
}

void AggregateHashTable::updateDistinctAggState(const std::vector<ValueVector*>& flatKeyVectors,
    const std::vector<ValueVector*>& /*unFlatKeyVectors*/, AggregateFunction& aggregateFunction,
    ValueVector* aggregateVector, uint64_t /*multiplicity*/, uint32_t colIdx,
    uint32_t aggStateOffset) {
    auto distinctHT = distinctHashTables[colIdx].get();
    KU_ASSERT(distinctHT != nullptr);
    if (distinctHT->isAggregateValueDistinctForGroupByKeys(flatKeyVectors, aggregateVector)) {
        auto pos = aggregateVector->state->getSelVector()[0];
        if (!aggregateVector->isNull(pos)) {
            aggregateFunction.updatePosState(
                hashSlotsToUpdateAggState[flatKeyVectors.empty() ?
                                              0 :
                                              flatKeyVectors[0]->state->getSelVector()[0]]
                        ->entry +
                    aggStateOffset,
                aggregateVector, 1 /* Distinct aggregate should ignore multiplicity
                                          since they are known to be non-distinct. */
                ,
                pos, memoryManager);
        }
    }
}

void AggregateHashTable::updateAggState(const std::vector<ValueVector*>& flatKeyVectors,
    const std::vector<ValueVector*>& unFlatKeyVectors, AggregateFunction& aggregateFunction,
    ValueVector* aggVector, uint64_t multiplicity, uint32_t /*colIdx*/, uint32_t aggStateOffset) {
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
        aggregateStateOffset += aggregateFunctions[i].getAggregateStateSize();
    }
}

void AggregateHashTable::fillEntryWithInitialNullAggregateState(FactorizedTable& table,
    uint8_t* entry) {
    for (auto i = 0u; i < aggregateFunctions.size(); i++) {
        table.updateFlatCellNoNull(entry, aggStateColIdxInFT + i,
            (void*)aggregateFunctions[i].getInitialNullAggregateState());
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
    auto numHashSlotsPerBlock = static_cast<uint64_t>(1) << numSlotsPerBlockLog2;
    auto numHashSlotsBlocksNeeded =
        (maxNumHashSlots + numHashSlotsPerBlock - 1) / numHashSlotsPerBlock;
    while (hashSlotsBlocks.size() < numHashSlotsBlocksNeeded) {
        hashSlotsBlocks.emplace_back(std::make_unique<DataBlock>(memoryManager, HASH_BLOCK_SIZE));
    }
}

void AggregateHashTable::resizeHashTableIfNecessary(uint32_t maxNumDistinctHashKeys) {
    if (factorizedTable->getNumTuples() + maxNumDistinctHashKeys > maxNumHashSlots ||
        static_cast<double>(factorizedTable->getNumTuples()) + maxNumDistinctHashKeys >
            static_cast<double>(maxNumHashSlots) / DEFAULT_HT_LOAD_FACTOR) {
        resize(std::bit_ceil(static_cast<uint64_t>(
            (factorizedTable->getNumTuples() + maxNumDistinctHashKeys) * DEFAULT_HT_LOAD_FACTOR)));
    }
}

void AggregateHashTable::updateNullAggVectorState(const std::vector<ValueVector*>& flatKeyVectors,
    const std::vector<ValueVector*>& unFlatKeyVectors, AggregateFunction& aggregateFunction,
    uint64_t multiplicity, uint32_t aggStateOffset) {
    if (unFlatKeyVectors.empty()) {
        auto pos = flatKeyVectors[0]->state->getSelVector()[0];
        aggregateFunction.updatePosState(hashSlotsToUpdateAggState[pos]->entry + aggStateOffset,
            nullptr, multiplicity, 0 /* dummy pos */, memoryManager);
    } else {
        unFlatKeyVectors[0]->state->getSelVector().forEach([&](auto pos) {
            aggregateFunction.updatePosState(hashSlotsToUpdateAggState[pos]->entry + aggStateOffset,
                nullptr, multiplicity, 0 /* dummy pos */, memoryManager);
        });
    }
}

void AggregateHashTable::updateBothFlatAggVectorState(
    const std::vector<ValueVector*>& /*flatKeyVectors*/, AggregateFunction& aggregateFunction,
    ValueVector* aggVector, uint64_t multiplicity, uint32_t aggStateOffset) {
    auto aggPos = aggVector->state->getSelVector()[0];
    if (!aggVector->isNull(aggPos)) {
        aggregateFunction.updatePosState(
            hashSlotsToUpdateAggState[hashVector->state->getSelVector()[0]]->entry + aggStateOffset,
            aggVector, multiplicity, aggPos, memoryManager);
    }
}

void AggregateHashTable::updateFlatUnFlatKeyFlatAggVectorState(
    const std::vector<ValueVector*>& /*flatKeyVectors*/,
    const std::vector<ValueVector*>& unFlatKeyVectors, AggregateFunction& aggregateFunction,
    ValueVector* aggVector, uint64_t multiplicity, uint32_t aggStateOffset) {
    auto aggPos = aggVector->state->getSelVector()[0];
    if (!aggVector->isNull(aggPos)) {
        unFlatKeyVectors[0]->state->getSelVector().forEach([&](auto pos) {
            aggregateFunction.updatePosState(hashSlotsToUpdateAggState[pos]->entry + aggStateOffset,
                aggVector, multiplicity, aggPos, memoryManager);
        });
    }
}

void AggregateHashTable::updateFlatKeyUnFlatAggVectorState(
    const std::vector<ValueVector*>& flatKeyVectors, AggregateFunction& aggregateFunction,
    ValueVector* aggVector, uint64_t multiplicity, uint32_t aggStateOffset) {
    auto groupByKeyPos = flatKeyVectors[0]->state->getSelVector()[0];
    aggVector->forEachNonNull([&](auto pos) {
        aggregateFunction.updatePosState(hashSlotsToUpdateAggState[groupByKeyPos]->entry +
                                             aggStateOffset,
            aggVector, multiplicity, pos, memoryManager);
    });
}

void AggregateHashTable::updateBothUnFlatSameDCAggVectorState(
    const std::vector<ValueVector*>& /*flatKeyVectors*/,
    const std::vector<ValueVector*>& /*unFlatKeyVectors*/, AggregateFunction& aggregateFunction,
    ValueVector* aggVector, uint64_t multiplicity, uint32_t aggStateOffset) {
    aggVector->forEachNonNull([&](auto pos) {
        aggregateFunction.updatePosState(hashSlotsToUpdateAggState[pos]->entry + aggStateOffset,
            aggVector, multiplicity, pos, memoryManager);
    });
}

void AggregateHashTable::updateBothUnFlatDifferentDCAggVectorState(
    const std::vector<ValueVector*>& /*flatKeyVectors*/,
    const std::vector<ValueVector*>& unFlatKeyVectors, AggregateFunction& aggregateFunction,
    ValueVector* aggVector, uint64_t multiplicity, uint32_t aggStateOffset) {
    unFlatKeyVectors[0]->state->getSelVector().forEach([&](auto pos) {
        aggregateFunction.updateAllState(hashSlotsToUpdateAggState[pos]->entry + aggStateOffset,
            aggVector, multiplicity, memoryManager);
    });
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

uint64_t PartitioningAggregateHashTable::append(const std::vector<ValueVector*>& flatKeyVectors,
    const std::vector<ValueVector*>& unFlatKeyVectors,
    const std::vector<ValueVector*>& dependentKeyVectors, DataChunkState* leadingState,
    const std::vector<AggregateInput>& aggregateInputs, uint64_t resultSetMultiplicity) {
    const auto numFlatTuples = leadingState->getSelVector().getSelSize();
    computeVectorHashes(flatKeyVectors, unFlatKeyVectors);

    auto startingNumTuples = getNumEntries();
    if (startingNumTuples + numFlatTuples > maxNumHashSlots ||
        (double)startingNumTuples + numFlatTuples >
            (double)maxNumHashSlots / DEFAULT_HT_LOAD_FACTOR) {
        mergeAll();
    }
    findHashSlots(flatKeyVectors, unFlatKeyVectors, dependentKeyVectors, leadingState);
    updateAggStates(flatKeyVectors, unFlatKeyVectors, aggregateInputs, resultSetMultiplicity);
    return numFlatTuples;
}

void PartitioningAggregateHashTable::mergeAll() {
    for (uint64_t i = 0; i < factorizedTable->getNumTuples(); i++) {
        auto* tuple = factorizedTable->getTuple(i);
        auto hash = *(hash_t*)(tuple + tableSchema.getColOffset(tableSchema.getNumColumns() - 1));
        sharedState->appendTuple(std::span(tuple, tableSchema.getNumBytesPerTuple()), hash);
    }
    // Move overflow data into the shared state so that it isn't obliterated when we clear the
    // factorized table
    sharedState->appendOverflow(std::move(*factorizedTable->getInMemOverflowBuffer()));

    factorizedTable->clear();
    // Clear hash table
    for (auto& block : hashSlotsBlocks) {
        block->resetToZero();
    }
}

} // namespace processor
} // namespace kuzu
