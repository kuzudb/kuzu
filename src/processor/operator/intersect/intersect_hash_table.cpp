#include "include/intersect_hash_table.h"

namespace graphflow {
namespace processor {

static void sortSelectedPos(const shared_ptr<ValueVector>& nodeIDVector) {
    auto selVector = nodeIDVector->state->selVector.get();
    auto size = selVector->selectedSize;
    auto selectedPos = selVector->getSelectedPositionsBuffer();
    if (selVector->isUnfiltered()) {
        memcpy(selectedPos, &SelectionVector::INCREMENTAL_SELECTED_POS, size * sizeof(sel_t));
        selVector->resetSelectorToValuePosBuffer();
    }
    sort(selectedPos, selectedPos + size, [nodeIDVector](sel_t left, sel_t right) {
        return nodeIDVector->readNodeOffset(left) < nodeIDVector->readNodeOffset(right);
    });
}

void IntersectHashTable::append(const vector<shared_ptr<ValueVector>>& vectorsToAppend) {
    auto numTuplesToAppend = 1, numKeyColumns = 1;
    if (!discardNullFromKeys(vectorsToAppend, numKeyColumns)) {
        return;
    }
    assert(vectorsToAppend[0]->state->isFlat());
    sortSelectedPos(vectorsToAppend[1]);
    auto appendInfos = factorizedTable->allocateFlatTupleBlocks(numTuplesToAppend);
    assert(appendInfos.size() == 1);
    for (auto i = 0u; i < vectorsToAppend.size(); i++) {
        factorizedTable->copyVectorToColumn(
            *vectorsToAppend[i], appendInfos[0], numTuplesToAppend, i);
    }
    factorizedTable->numTuples += numTuplesToAppend;
}

} // namespace processor
} // namespace graphflow
