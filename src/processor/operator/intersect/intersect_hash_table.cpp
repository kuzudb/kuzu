#include "processor/operator/intersect/intersect_hash_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

static void sortSelectedPos(ValueVector* nodeIDVector) {
    auto selVector = nodeIDVector->state->selVector.get();
    auto size = selVector->selectedSize;
    auto selectedPos = selVector->getSelectedPositionsBuffer();
    if (selVector->isUnfiltered()) {
        memcpy(selectedPos, &SelectionVector::INCREMENTAL_SELECTED_POS, size * sizeof(sel_t));
        selVector->resetSelectorToValuePosBuffer();
    }
    std::sort(selectedPos, selectedPos + size, [nodeIDVector](sel_t left, sel_t right) {
        return nodeIDVector->getValue<nodeID_t>(left) < nodeIDVector->getValue<nodeID_t>(right);
    });
}

void IntersectHashTable::append(const std::vector<ValueVector*>& vectorsToAppend) {
    auto numTuplesToAppend = 1;
    // Based on the way we are planning, we assume that the first and second vectors are both
    // nodeIDs from extending, while the first one is key, and the second one is payload.
    auto keyState = vectorsToAppend[0]->state.get();
    auto payloadNodeIDVector = vectorsToAppend[1];
    auto payloadsState = payloadNodeIDVector->state.get();
    assert(keyState->isFlat());
    if (!payloadsState->isFlat()) {
        // Sorting is only needed when the payload is unflat (a list of values).
        sortSelectedPos(payloadNodeIDVector);
    }
    // A single appendInfo will return from `allocateFlatTupleBlocks` when numTuplesToAppend is 1.
    auto appendInfos = factorizedTable->allocateFlatTupleBlocks(numTuplesToAppend);
    assert(appendInfos.size() == 1);
    for (auto i = 0u; i < vectorsToAppend.size(); i++) {
        factorizedTable->copyVectorToColumn(
            *vectorsToAppend[i], appendInfos[0], numTuplesToAppend, i);
    }
    if (!payloadsState->isFlat()) {
        payloadsState->selVector->resetSelectorToUnselected();
    }
    factorizedTable->numTuples += numTuplesToAppend;
}

} // namespace processor
} // namespace kuzu
