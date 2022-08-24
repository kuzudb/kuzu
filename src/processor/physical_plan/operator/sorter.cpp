#include "src/processor/include/physical_plan/operator/sorter.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> Sorter::init(graphflow::processor::ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    vectorToSort = resultSet->getValueVector(vectorToSortPos);
    return resultSet;
}

bool Sorter::getNextTuples() {
    metrics->executionTime.start();
    restoreSelVector(vectorToSort->state->selVector.get());
    if (!children[0]->getNextTuples()) {
        metrics->executionTime.stop();
        return false;
    }
    saveSelVector(vectorToSort->state->selVector.get());
    assert(!vectorToSort->state->isFlat());
    auto selVector = vectorToSort->state->selVector.get();
    auto size = selVector->selectedSize;
    auto selectedPos = selVector->getSelectedPositionsBuffer();
    if (selVector->isUnfiltered()) {
        memcpy(selectedPos, &SelectionVector::INCREMENTAL_SELECTED_POS, size * sizeof(sel_t));
        selVector->resetSelectorToValuePosBuffer();
    }
    sort(selectedPos, selectedPos + size, [&](sel_t left, sel_t right) {
      return vectorToSort->readNodeOffset(left) < vectorToSort->readNodeOffset(right);
    });
    metrics->executionTime.stop();
    metrics->numOutputTuple.incrementByOne();
    return true;
}

} // namespace processor
} // namespace graphflow
