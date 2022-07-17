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
    return resultSet;
}

void Intersect::reInitToRerunSubPlan() {
    children[0]->reInitToRerunSubPlan();
    FilteringOperator::reInitToRerunSubPlan();
    leftIdx = 0;
}

static void sortSelectedPos(const shared_ptr<ValueVector>& nodeIDVector) {
    auto selectedPos = nodeIDVector->state->selVector->getSelectedPositionsBuffer();
    auto size = nodeIDVector->state->selVector->selectedSize;
    sort(selectedPos, selectedPos + size, [nodeIDVector](sel_t left, sel_t right) {
        return nodeIDVector->readNodeOffset(left) < nodeIDVector->readNodeOffset(right);
    });
}

bool Intersect::getNextTuples() {
    metrics->executionTime.start();
    auto numSelectedValues = 0u;
    do {
        restoreDataChunkSelectorState(leftDataChunk);
        if (rightDataChunk->state->currIdx == -1 || rightDataChunk->state->isCurrIdxLast()) {
            rightDataChunk->state->currIdx = -1;
            if (!children[0]->getNextTuples()) {
                metrics->executionTime.stop();
                return false;
            }
            sortSelectedPos(leftValueVector);
            sortSelectedPos(rightValueVector);
            leftIdx = 0;
        }
        // Flatten right dataChunk
        rightDataChunk->state->currIdx++;
        saveDataChunkSelectorState(leftDataChunk);
        auto rightPos = rightDataChunk->state->getPositionOfCurrIdx();
        auto rightNodeOffset = rightValueVector->readNodeOffset(rightPos);
        numSelectedValues = 0u;
        while (leftIdx < leftDataChunk->state->selVector->selectedSize) {
            auto leftPos = leftDataChunk->state->selVector->selectedPositions[leftIdx];
            auto leftNodeOffset = leftValueVector->readNodeOffset(leftPos);
            if (leftNodeOffset > rightNodeOffset) {
                break;
            } else if (leftNodeOffset == rightNodeOffset) {
                leftDataChunk->state->selVector->getSelectedPositionsBuffer()[numSelectedValues++] =
                    leftPos;
            }
            leftIdx++;
        }
        leftIdx -= numSelectedValues; // restore leftIdx to the first match
        leftDataChunk->state->selVector->selectedSize = numSelectedValues;
        if (leftDataChunk->state->selVector->isUnfiltered()) {
            leftDataChunk->state->selVector->resetSelectorToValuePosBuffer();
        }
    } while (numSelectedValues == 0u);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(numSelectedValues);
    return true;
}

} // namespace processor
} // namespace graphflow
