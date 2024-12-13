#include "processor/operator/hash_join/anti_join_hash_table.h"

namespace kuzu {
namespace processor {

void AntiJoinProbeState::initState(uint64_t maxNumHashSlots_, uint64_t hashSlotIdx_) {
    nextHashSlotIdx = 0;
    maxNumHashSlots = maxNumHashSlots_;
    hashSlotIdx = hashSlotIdx_;
}

bool AntiJoinProbeState::hasMoreToProbe() const {
    return nextHashSlotIdx < maxNumHashSlots;
}

bool AntiJoinProbeState::checkKeys() const {
    return nextHashSlotIdx - 1 == hashSlotIdx;
}

void AntiJoinHashTable::initProbeState(kuzu::processor::AntiJoinProbeState& probeState,
    const std::vector<common::ValueVector*>& keyVectors, common::ValueVector& hashVector,
    common::SelectionVector& hashSelVec, common::ValueVector& tmpHashResultVector) const {
    KU_ASSERT(keyVectors.size() == keyTypes.size());
    KU_ASSERT(keyVectors[0]->state->isFlat());
    computeHashValue(keyVectors, hashVector, tmpHashResultVector, hashSelVec);
    probeState.initState(maxNumHashSlots,
        getSlotIdxForHash(hashVector.getValue<common::hash_t>(0)));
}

void AntiJoinHashTable::probe(AntiJoinProbeState& probeState, uint8_t** probedTuples) const {
    probedTuples[0] =
        ((uint8_t**)(hashSlotsBlocks[probeState.nextHashSlotIdx >> numSlotsPerBlockLog2]
                ->getData()))[probeState.nextHashSlotIdx & slotIdxInBlockMask];
    probeState.nextHashSlotIdx++;
}

} // namespace processor
} // namespace kuzu
