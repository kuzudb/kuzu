#pragma once

#include "src/processor/operator/hash_join/include/join_hash_table.h"

namespace kuzu {
namespace processor {

class IntersectHashTable : public JoinHashTable {
public:
    IntersectHashTable(MemoryManager& memoryManager, unique_ptr<FactorizedTableSchema> tableSchema)
        : JoinHashTable{memoryManager, 1 /* numKeyColumns */, move(tableSchema)} {}

    void append(const vector<shared_ptr<ValueVector>>& vectorsToAppend) override;
};

} // namespace processor
} // namespace kuzu
