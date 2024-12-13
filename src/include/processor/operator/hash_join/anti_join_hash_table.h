#pragma once

#include "join_hash_table.h"

namespace kuzu {
namespace processor {

struct AntiJoinProbeState {
    uint64_t nextHashSlotIdx = 0;
    uint64_t maxNumHashSlots = 0;
    uint64_t hashSlotIdx = 0;

    AntiJoinProbeState() = default;
    void initState(uint64_t maxNumHashSlots_, uint64_t hashSlotIdx_);
    bool hasMoreToProbe() const;
    bool checkKeys() const;
};

class AntiJoinHashTable : public JoinHashTable {
public:
    AntiJoinHashTable(storage::MemoryManager& memoryManager, common::logical_type_vec_t keyTypes,
        FactorizedTableSchema tableSchema)
        : JoinHashTable{memoryManager, std::move(keyTypes), std::move(tableSchema)} {}

    void initProbeState(AntiJoinProbeState& probeState,
        const std::vector<common::ValueVector*>& keyVectors, common::ValueVector& hashVector,
        common::SelectionVector& hashSelVec, common::ValueVector& tmpHashResultVector) const;

    void probe(AntiJoinProbeState& probeState, uint8_t** probedTuples) const;
};

} // namespace processor
} // namespace kuzu
