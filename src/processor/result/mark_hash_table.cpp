#include "processor/result/mark_hash_table.h"

namespace kuzu {
namespace processor {

MarkHashTable::MarkHashTable(storage::MemoryManager& memoryManager,
    std::vector<common::LogicalType> keyTypes, std::vector<common::LogicalType> payloadTypes,
    uint64_t numEntriesToAllocate, FactorizedTableSchema tableSchema)
    : AggregateHashTable(memoryManager, std::move(keyTypes), std::move(payloadTypes),
          std::vector<std::unique_ptr<function::AggregateFunction>>{} /* empty aggregates */,
          std::vector<common::LogicalType>{} /* empty distinct agg key*/, numEntriesToAllocate,
          std::move(tableSchema)) {
    distinctColIdxInFT = hashColIdxInFT - 1;
}

uint64_t MarkHashTable::matchFTEntries(const std::vector<common::ValueVector*>& flatKeyVectors,
    const std::vector<common::ValueVector*>& unFlatKeyVectors, uint64_t numMayMatches,
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
    for (auto i = 0u; i < numMayMatches; i++) {
        noMatchIdxes[numNoMatches++] = mayMatchIdxes[i];
        onMatchSlotIdxes.emplace(mayMatchIdxes[i]);
    }
    return numNoMatches;
}

void MarkHashTable::initializeFTEntries(const std::vector<common::ValueVector*>& flatKeyVectors,
    const std::vector<common::ValueVector*>& unFlatKeyVectors,
    const std::vector<common::ValueVector*>& dependentKeyVectors,
    uint64_t numFTEntriesToInitialize) {
    AggregateHashTable::initializeFTEntries(flatKeyVectors, unFlatKeyVectors, dependentKeyVectors,
        numFTEntriesToInitialize);
    for (auto i = 0u; i < numFTEntriesToInitialize; i++) {
        auto entryIdx = entryIdxesToInitialize[i];
        auto entry = hashSlotsToUpdateAggState[entryIdx]->entry;
        auto onMatch = !onMatchSlotIdxes.contains(entryIdx);
        onMatchSlotIdxes.erase(entryIdx);
        factorizedTable->updateFlatCellNoNull(entry, distinctColIdxInFT, &onMatch /* isOnMatch */);
    }
}

} // namespace processor
} // namespace kuzu
