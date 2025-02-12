#include "processor/operator/aggregate/aggregate_hash_table.h"

#include <cstdint>
#include <memory>

#include "common/assert.h"
#include "common/constants.h"
#include "common/data_chunk/data_chunk_state.h"
#include "common/in_mem_overflow_buffer.h"
#include "common/type_utils.h"
#include "common/types/types.h"
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
    distinctHashTables.reserve(this->aggregateFunctions.size());
    distinctHashEntriesProcessed.resize(this->aggregateFunctions.size());
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
    updateAggStates(flatKeyVectors, unFlatKeyVectors, aggregateInputs, resultSetMultiplicity,
        true /*updateDistinct*/);
    return numFlatTuples;
}

bool AggregateHashTable::insertAggregateValueIfDistinctForGroupByKeys(
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
            // We'll update the distinct state at the end.
            // The distinct data gets merged separately, and only after the main data so that
            // we can guarantee that there is a group available in teh hash table for any given
            // distinct tuple.
            if (!aggregateFunction.isDistinct) {
                for (auto i = 0u; i < numTuplesToScan; i++) {
                    aggregateFunction.combineState(hashSlotsToUpdateAggState[i]->entry +
                                                       aggregateStateOffset,
                        table.getTuple(startTupleIdx + i) + aggregateStateOffset, memoryManager);
                }
            }
            aggregateStateOffset += aggregateFunction.getAggregateStateSize();
        }
        startTupleIdx += numTuplesToScan;
    }
}

void AggregateHashTable::mergeDistinctAggregateInfo() {
    auto state = std::make_shared<DataChunkState>();
    std::vector<std::unique_ptr<ValueVector>> vectors;
    std::vector<ValueVector*> vectorPtrs;
    vectors.reserve(keyTypes.size() + 1);
    vectorPtrs.reserve(keyTypes.size() + 1);
    for (auto& keyType : keyTypes) {
        vectors.emplace_back(std::make_unique<ValueVector>(keyType.copy(), memoryManager, state));
        vectorPtrs.emplace_back(vectors.back().get());
    }
    vectors.emplace_back(nullptr);
    vectorPtrs.emplace_back(nullptr);

    auto aggStateColOffset = aggStateColOffsetInFT;
    for (size_t distinctIdx = 0; distinctIdx < distinctHashTables.size(); distinctIdx++) {
        auto& distinctHashTable = distinctHashTables[distinctIdx];
        if (distinctHashTable) {
            // Distinct key type is always the last key type in the distinct table
            vectors.back() = std::make_unique<ValueVector>(
                distinctHashTable->keyTypes.back().copy(), memoryManager, state);
            vectorPtrs.back() = vectors.back().get();

            // process 2048 at a time, beginning with the first unprocessed tuple
            while (distinctHashEntriesProcessed[distinctIdx] < distinctHashTable->getNumEntries()) {
                // Scan everything but the hash column, which isn't needed as we need hashes for
                // everything but the distinct key type
                std::vector<uint32_t> colIdxToScan(vectorPtrs.size());
                std::iota(colIdxToScan.begin(), colIdxToScan.end(), 0);

                auto numTuplesToScan = std::min(DEFAULT_VECTOR_CAPACITY,
                    distinctHashTable->getNumEntries() - distinctHashEntriesProcessed[distinctIdx]);
                state->getSelVectorUnsafe().setSelSize(numTuplesToScan);
                distinctHashTable->factorizedTable->scan(vectorPtrs,
                    distinctHashEntriesProcessed[distinctIdx], numTuplesToScan, colIdxToScan);
                // Compute hashes for the scanned keys but not the distinct key (i.e. the groups in
                // the main hash table)
                computeVectorHashes(std::span<const ValueVector*>{},
                    std::span(const_cast<const ValueVector**>(vectorPtrs.data()),
                        vectorPtrs.size() - 1));
                for (size_t i = 0; i < numTuplesToScan; i++) {
                    // Find slot in current hash table corresponding to the entry in the distinct
                    // hash table
                    auto hash = hashVector->getValue<hash_t>(i);
                    // matchFTEntries expects that the index is the same as the index in the vector
                    // Doesn't need to be done each time, but maybe this can be vectorized so that
                    // we can find the correct slots for all of them simultaneously?
                    mayMatchIdxes[0] = i;
                    auto entry = findEntry(hash, [&](auto entry) {
                        HashSlot slot{hash, entry};
                        hashSlotsToUpdateAggState[i] = &slot;
                        return matchFTEntries(std::span<const ValueVector*>{},
                                   std::span(const_cast<const ValueVector**>(vectorPtrs.data()),
                                       vectorPtrs.size() - 1),
                                   1, 0) == 0;
                    });
                    KU_ASSERT(entry != nullptr);
                    aggregateFunctions[distinctIdx].updatePosState(entry + aggStateColOffset,
                        vectors.back().get() /*aggregateVector*/,
                        1 /* Distinct aggregate should ignore multiplicity since they are known to
                             be non-distinct. */
                        ,
                        i, memoryManager);
                }
                distinctHashEntriesProcessed[distinctIdx] += numTuplesToScan;
            }
        }
        aggStateColOffset += aggregateFunctions[distinctIdx].getAggregateStateSize();
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
    return findEntry(hash,
        [&](auto entry) { return matchFlatVecWithEntry(groupByKeyVectors, entry); });
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

uint64_t AggregateHashTable::matchFTEntries(std::span<const ValueVector*> flatKeyVectors,
    std::span<const ValueVector*> unFlatKeyVectors, uint64_t numMayMatches, uint64_t numNoMatches) {
    auto colIdx = 0u;
    for (const auto& flatKeyVector : flatKeyVectors) {
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
            if (ftCompareEntryFuncs[colIdx](srcTable.getTuple(startOffset + idx) + colOffset,
                    hashSlotsToUpdateAggState[idx]->entry + colOffset, keyTypes[colIdx])) {
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

uint64_t AggregateHashTable::matchUnFlatVecWithFTColumn(const ValueVector* vector,
    uint64_t numMayMatches, uint64_t& numNoMatches, uint32_t colIdx) {
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

uint64_t AggregateHashTable::matchFlatVecWithFTColumn(const ValueVector* vector,
    uint64_t numMayMatches, uint64_t& numNoMatches, uint32_t colIdx) {
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
    slotIdx = (slotIdx + 1) & bitmask;
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
        numNoMatches = matchFTEntries(constSpan(flatKeyVectors), constSpan(unFlatKeyVectors),
            numMayMatches, numNoMatches);
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
    if (distinctHT->insertAggregateValueIfDistinctForGroupByKeys(flatKeyVectors, aggregateVector)) {
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
    const std::vector<AggregateInput>& aggregateInputs, uint64_t resultSetMultiplicity,
    bool updateDistinct) {
    auto aggregateStateOffset = aggStateColOffsetInFT;
    for (auto i = 0u; i < aggregateFunctions.size(); i++) {
        if (!aggregateFunctions[i].isDistinct || updateDistinct) {
            auto multiplicity = resultSetMultiplicity;
            for (auto& dataChunk : aggregateInputs[i].multiplicityChunks) {
                multiplicity *= dataChunk->state->getSelVector().getSelSize();
            }
            updateAggFuncs[i](this, flatKeyVectors, unFlatKeyVectors, aggregateFunctions[i],
                aggregateInputs[i].aggregateVector, multiplicity, i, aggregateStateOffset);
            aggregateStateOffset += aggregateFunctions[i].getAggregateStateSize();
        } else {
            // If a function is distinct we still need to insert the value into the distinct hash
            // table
            distinctHashTables[i]->insertAggregateValueIfDistinctForGroupByKeys(flatKeyVectors,
                aggregateInputs[i].aggregateVector);
        }
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
        // This isn't really necessary except in the global queues, but it's easier to just always
        // set it here
        columnSchema.setMayContainsNullsToTrue();
        tableSchema.appendColumn(std::move(columnSchema));
    }
    // Distinct key column
    hashKeyTypes[i] = distinctKeyType.copy();
    auto columnSchema = ColumnSchema(false /* isUnFlat */, 0 /* groupID */,
        LogicalTypeUtils::getRowLayoutSize(distinctKeyType));
    columnSchema.setMayContainsNullsToTrue();
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

    auto startingNumTuples = getNumEntries();
    if (startingNumTuples + numFlatTuples > maxNumHashSlots ||
        (double)startingNumTuples + numFlatTuples >
            (double)maxNumHashSlots / DEFAULT_HT_LOAD_FACTOR) {
        mergeAll();
    }

    // mergeAll makes use of the hashVector, so it needs to be called before computeVectorHashes
    computeVectorHashes(flatKeyVectors, unFlatKeyVectors);
    findHashSlots(flatKeyVectors, unFlatKeyVectors, dependentKeyVectors, leadingState);
    // Don't update distinct states since they can't be merged into the global hash tables. Instead
    // we'll calculate them from scratch when merging.
    updateAggStates(flatKeyVectors, unFlatKeyVectors, aggregateInputs, resultSetMultiplicity,
        false /*updateDistinct*/);
    return numFlatTuples;
}

void PartitioningAggregateHashTable::mergeAll() {
    for (uint64_t i = 0; i < factorizedTable->getNumTuples(); i++) {
        auto* tuple = factorizedTable->getTuple(i);
        auto hash = *(hash_t*)(tuple + tableSchema.getColOffset(tableSchema.getNumColumns() - 1));
        sharedState->appendTuple(std::span(tuple, tableSchema.getNumBytesPerTuple()), hash);
    }
    std::vector<ValueVector*> vectors;
    std::vector<std::unique_ptr<ValueVector>> keyVectors;
    std::vector<ft_col_idx_t> colIdxToScan;
    if (distinctHashTables.size() > 0) {
        // We need the hashes of the key columns to partition them appropriately.
        // These will be the same for each of the distinct hash tables since we exclude the distinct
        // aggregate key Reserve inside here so that we don't unnecessarily allocate memory if there
        // are no distinct hash tables
        colIdxToScan.resize(keyTypes.size());
        std::iota(colIdxToScan.begin(), colIdxToScan.end(), 0);
        vectors.reserve(keyTypes.size());
        keyVectors.reserve(keyTypes.size());
        auto state = std::make_shared<DataChunkState>();

        for (const auto& keyType : keyTypes) {
            keyVectors.push_back(
                std::make_unique<ValueVector>(keyType.copy(), memoryManager, state));
            vectors.push_back(keyVectors.back().get());
        }
    }
    for (size_t distinctIdx = 0; distinctIdx < distinctHashTables.size(); distinctIdx++) {
        if (distinctHashTables[distinctIdx]) {
            auto* distinctFactorizedTable = distinctHashTables[distinctIdx]->getFactorizedTable();
            auto distinctTableSchema = distinctFactorizedTable->getTableSchema();
            uint64_t startTupleIdx = 0;
            auto numTuplesToScan =
                std::min(distinctFactorizedTable->getNumTuples(), DEFAULT_VECTOR_CAPACITY);
            while (startTupleIdx < distinctFactorizedTable->getNumTuples()) {
                distinctFactorizedTable->scan(vectors, startTupleIdx, numTuplesToScan,
                    colIdxToScan);
                computeVectorHashes(std::vector<ValueVector*>{}, vectors);
                for (uint64_t tupleIdx = 0; tupleIdx < numTuplesToScan; tupleIdx++) {
                    auto* tuple = distinctFactorizedTable->getTuple(startTupleIdx + tupleIdx);
                    // The distinct value needs to be partitioned according to the group that stores
                    // its aggregate state So we need to ignore the aggregate key when calculating
                    // the hash for partitioning
                    const auto hash = hashVector->getValue<hash_t>(tupleIdx);
                    sharedState->appendDistinctTuple(distinctIdx,
                        std::span(tuple, distinctTableSchema->getNumBytesPerTuple()), hash);
                }
                startTupleIdx += numTuplesToScan;
                numTuplesToScan = std::min(distinctFactorizedTable->getNumTuples() - startTupleIdx,
                    DEFAULT_VECTOR_CAPACITY);
            }

            sharedState->appendOverflow(
                std::move(*distinctFactorizedTable->getInMemOverflowBuffer()));
            distinctHashTables[distinctIdx]->clear();
        }
    }
    // Move overflow data into the shared state so that it isn't obliterated when we clear the
    // factorized table
    sharedState->appendOverflow(std::move(*factorizedTable->getInMemOverflowBuffer()));

    clear();
}

void AggregateHashTable::clear() {
    factorizedTable->clear();
    // Clear hash table
    for (auto& block : hashSlotsBlocks) {
        block->resetToZero();
    }
}

} // namespace processor
} // namespace kuzu
