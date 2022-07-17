#include "src/processor/include/physical_plan/operator/filtering_operator.h"

namespace graphflow {
namespace processor {

void FilteringOperator::restoreDataChunkSelectorState(
    const shared_ptr<DataChunk>& dataChunkToSelect) {
    if (prevSelVector->selectedPositions == nullptr) {
        return;
    }
    dataChunkToSelect->state->selVector->selectedSize = prevSelVector->selectedSize;
    if (prevSelVector->isUnfiltered()) {
        dataChunkToSelect->state->selVector->resetSelectorToUnselected();
    } else {
        dataChunkToSelect->state->selVector->resetSelectorToValuePosBuffer();
        memcpy(dataChunkToSelect->state->selVector->selectedPositions,
            prevSelVector->getSelectedPositionsBuffer(),
            prevSelVector->selectedSize * sizeof(sel_t));
    }
}

void FilteringOperator::saveDataChunkSelectorState(const shared_ptr<DataChunk>& dataChunkToSelect) {
    prevSelVector->selectedSize = dataChunkToSelect->state->selVector->selectedSize;
    if (dataChunkToSelect->state->selVector->isUnfiltered()) {
        prevSelVector->resetSelectorToUnselected();
    } else {
        memcpy(prevSelVector->getSelectedPositionsBuffer(),
            dataChunkToSelect->state->selVector->getSelectedPositionsBuffer(),
            prevSelVector->selectedSize * sizeof(sel_t));
        prevSelVector->resetSelectorToValuePosBuffer();
    }
}
} // namespace processor
} // namespace graphflow
