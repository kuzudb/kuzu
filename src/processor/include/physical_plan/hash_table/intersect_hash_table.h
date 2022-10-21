#pragma once

#include "src/processor/include/physical_plan/hash_table/join_hash_table.h"

namespace graphflow {
namespace processor {

class IntersectHashTable : public JoinHashTable {
public:
    IntersectHashTable(MemoryManager& memoryManager, unique_ptr<FactorizedTableSchema> tableSchema)
        : JoinHashTable{memoryManager, 1 /* numKeyColumns */, move(tableSchema)},
          lastAppendedKey(UINT64_MAX, UINT64_MAX) {}

    void append(const vector<shared_ptr<ValueVector>>& vectorsToAppend) override;

private:
    nodeID_t lastAppendedKey;
};

} // namespace processor
} // namespace graphflow
