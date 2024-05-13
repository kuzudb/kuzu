#include "processor/operator/aggregate/aggregate_hash_table.h"

#pragma once

namespace kuzu {
namespace processor {

class MarkHashTable : public AggregateHashTable {
public:
    MarkHashTable(storage::MemoryManager& memoryManager, std::vector<common::LogicalType> keyTypes,
        std::vector<common::LogicalType> payloadTypes, uint64_t numEntriesToAllocate,
        FactorizedTableSchema tableSchema);

    uint64_t matchFTEntries(const std::vector<common::ValueVector*>& flatKeyVectors,
        const std::vector<common::ValueVector*>& unFlatKeyVectors, uint64_t numMayMatches,
        uint64_t numNoMatches) override;

    void initializeFTEntries(const std::vector<common::ValueVector*>& flatKeyVectors,
        const std::vector<common::ValueVector*>& unFlatKeyVectors,
        const std::vector<common::ValueVector*>& dependentKeyVectors,
        uint64_t numFTEntriesToInitialize) override;

private:
    std::unordered_set<uint64_t> onMatchSlotIdxes;
    uint32_t distinctColIdxInFT;
};

} // namespace processor
} // namespace kuzu
