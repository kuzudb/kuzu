#pragma once

#include "processor/operator/hash_join/join_hash_table.h"

namespace kuzu {
namespace processor {

class IntersectHashTable : public JoinHashTable {
public:
    IntersectHashTable(
        storage::MemoryManager& memoryManager, std::unique_ptr<FactorizedTableSchema> tableSchema)
        : JoinHashTable{memoryManager, 1 /* numKeyColumns */, std::move(tableSchema)} {}

    void append(const std::vector<common::ValueVector*>& vectorsToAppend) override;
};

} // namespace processor
} // namespace kuzu
