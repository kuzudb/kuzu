#include "src/processor/include/physical_plan/operator/intersect.h"

#include <algorithm>

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> Intersect::initResultSet() {
    resultSet = prevOperator->initResultSet();
    leftDataChunk = resultSet->dataChunks[leftDataPos.dataChunkPos];
    leftValueVector = leftDataChunk->valueVectors[leftDataPos.valueVectorPos];
    rightDataChunk = resultSet->dataChunks[rightDataPos.dataChunkPos];
    rightValueVector = rightDataChunk->valueVectors[rightDataPos.valueVectorPos];
    return resultSet;
}

static void sortSelectedPos(const shared_ptr<ValueVector>& nodeIDVector) {
    auto selectedPos = nodeIDVector->state->selectedPositionsBuffer.get();
    auto size = nodeIDVector->state->selectedSize;
    sort(selectedPos, selectedPos + size, [nodeIDVector](sel_t left, sel_t right) {
        return nodeIDVector->readNodeOffset(left) < nodeIDVector->readNodeOffset(right);
    });
}

bool Intersect::getNextTuples() {
    metrics->executionTime.start();
    auto numSelectedValues = 0u;
    do {
        restoreDataChunkSelectorState(leftDataChunk);
        if (rightDataChunk->state->currIdx == -1 ||
            rightDataChunk->state->selectedSize == rightDataChunk->state->currIdx + 1ul) {
            rightDataChunk->state->currIdx = -1;
            if (!prevOperator->getNextTuples()) {
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
        while (leftIdx < leftDataChunk->state->selectedSize) {
            auto leftPos = leftDataChunk->state->selectedPositions[leftIdx];
            auto leftNodeOffset = leftValueVector->readNodeOffset(leftPos);
            if (leftNodeOffset > rightNodeOffset) {
                break;
            } else if (leftNodeOffset == rightNodeOffset) {
                leftDataChunk->state->selectedPositionsBuffer[numSelectedValues++] = leftPos;
            }
            leftIdx++;
        }
        leftIdx -= numSelectedValues; // restore leftIdx to the first match
        leftDataChunk->state->selectedSize = numSelectedValues;
        if (leftDataChunk->state->isUnfiltered()) {
            leftDataChunk->state->resetSelectorToValuePosBuffer();
        }
    } while (numSelectedValues == 0u);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(numSelectedValues);
    return true;
}

} // namespace processor
} // namespace graphflow
