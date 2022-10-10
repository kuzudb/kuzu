#include "src/processor/include/physical_plan/operator/intersect.h"

#include <algorithm>

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> Intersect::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    leftDataChunk = resultSet->dataChunks[leftDataPos.dataChunkPos];
    leftValueVector = leftDataChunk->valueVectors[leftDataPos.valueVectorPos];
    rightDataChunk = resultSet->dataChunks[rightDataPos.dataChunkPos];
    rightValueVector = rightDataChunk->valueVectors[rightDataPos.valueVectorPos];
    selVectorsToSaveRestore.push_back(leftValueVector->state->selVector.get());
    selVectorsToSaveRestore.push_back(rightValueVector->state->selVector.get());
    return resultSet;
}

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

bool Intersect::getNextTuples() {
    metrics->executionTime.start();
    auto numSelectedValues = 0u;
    do {
        restoreSelVectors(selVectorsToSaveRestore);
        if (!children[0]->getNextTuples()) {
            metrics->executionTime.stop();
            return false;
        }
        assert(!leftValueVector->state->isFlat());
        assert(!rightValueVector->state->isFlat());
        saveSelVectors(selVectorsToSaveRestore);
        sortSelectedPos(leftValueVector);
        sortSelectedPos(rightValueVector);
        auto leftSelVector = leftValueVector->state->selVector.get();
        auto rightSelVector = rightValueVector->state->selVector.get();
        auto leftIdx = 0;
        auto rightIdx = 0;
        while (leftIdx < leftSelVector->selectedSize && rightIdx < rightSelVector->selectedSize) {
            auto leftPos = leftSelVector->selectedPositions[leftIdx];
            auto leftNodeOffset = leftValueVector->readNodeOffset(leftPos);
            auto rightPos = rightSelVector->selectedPositions[rightIdx];
            auto rightNodeOffset = rightValueVector->readNodeOffset(rightPos);
            if (leftNodeOffset > rightNodeOffset) {
                rightIdx++;
            } else if (leftNodeOffset < rightNodeOffset) {
                leftIdx++;
            } else {
                leftSelVector->getSelectedPositionsBuffer()[numSelectedValues++] = leftPos;
                leftIdx++;
                rightIdx++;
            }
        }
        leftSelVector->selectedSize = numSelectedValues;
    } while (numSelectedValues == 0);
    // TODO: open an issue about this
    rightValueVector->state->selVector->selectedSize = 1;
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
