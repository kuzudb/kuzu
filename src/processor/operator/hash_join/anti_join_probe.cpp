#include "processor/operator/hash_join/anti_join_probe.h"

#include "processor/operator/hash_join/anti_join_hash_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

bool AntiJoinProbe::getMatchedTuples(kuzu::processor::ExecutionContext* context) {
    KU_ASSERT(flatProbe);
    if (probeState->nextMatchedTupleIdx < probeState->matchedSelVector.getSelSize()) {
        // Not all matched tuples have been shipped. Continue shipping.
        return true;
    }

    auto antiJoinHT = sharedState->getHashTable()->constPtrCast<AntiJoinHashTable>();
    auto probedTuples = probeState->probedTuples.get();
    if (probedTuples[0] == nullptr) {
        if (!antiJoinProbeState.hasMoreToProbe()) {
            restoreSelVector(*keyVectors[0]->state);
            if (!children[0]->getNextTuple(context)) {
                return false;
            }
            saveSelVector(*keyVectors[0]->state);
            antiJoinHT->initProbeState(antiJoinProbeState, keyVectors, *hashVector, hashSelVec,
                *tmpHashVector);
        }

        while (antiJoinProbeState.hasMoreToProbe()) {
            antiJoinHT->probe(antiJoinProbeState, probedTuples);
            if (probeState->probedTuples[0] != nullptr) {
                break;
            }
        }
    }

    moveProbedTuplesToMatched();
    return true;
}

void AntiJoinProbe::moveProbedTuplesToMatched() {
    uint64_t numMatchedTuples = 0;
    auto probedTuples = probeState->probedTuples.get();
    if (antiJoinProbeState.checkKeys()) {
        numMatchedTuples =
            sharedState->getHashTable()->matchFlatKeys(keyVectors, probeState->probedTuples.get(),
                probeState->matchedTuples.get(), joinType != common::JoinType::ANTI);
    } else {
        while (probedTuples[0]) {
            if (numMatchedTuples == DEFAULT_VECTOR_CAPACITY) {
                break;
            }
            auto currentTuple = probedTuples[0];
            probeState->matchedTuples[numMatchedTuples] = currentTuple;
            numMatchedTuples++;
            probedTuples[0] = *sharedState->getHashTable()->getPrevTuple(currentTuple);
        }
    }
    probeState->matchedSelVector.setSelSize(numMatchedTuples);
    probeState->nextMatchedTupleIdx = 0;
}

} // namespace processor
} // namespace kuzu
