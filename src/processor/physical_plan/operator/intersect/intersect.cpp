#include "src/processor/include/physical_plan/operator/intersect/intersect.h"

#include <algorithm>

namespace graphflow {
namespace processor {

Intersect::Intersect(uint64_t leftDataChunkPos, uint64_t leftValueVectorPos,
    uint64_t rightDataChunkPos, uint64_t rightValueVectorPos,
    unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
    : PhysicalOperator{move(prevOperator), INTERSECT, context, id},
      FilteringOperator(this->prevOperator->getResultSet()->dataChunks[leftDataChunkPos]),
      leftDataChunkPos{leftDataChunkPos}, leftValueVectorPos{leftValueVectorPos},
      rightDataChunkPos{rightDataChunkPos}, rightValueVectorPos{rightValueVectorPos} {
    resultSet = this->prevOperator->getResultSet();
    leftDataChunk = resultSet->dataChunks[leftDataChunkPos];
    leftNodeIDVector = leftDataChunk->getValueVector(leftValueVectorPos);
    rightDataChunk = resultSet->dataChunks[rightDataChunkPos];
    rightNodeIDVector = rightDataChunk->getValueVector(rightValueVectorPos);
}

static void sortSelectedPos(const shared_ptr<ValueVector>& nodeIDVector) {
    auto selectedPos = nodeIDVector->state->selectedPositionsBuffer.get();
    auto size = nodeIDVector->state->selectedSize;
    sort(selectedPos, selectedPos + size, [nodeIDVector](sel_t left, sel_t right) {
        return nodeIDVector->readNodeOffset(left) < nodeIDVector->readNodeOffset(right);
    });
}

void Intersect::getNextTuples() {
    auto numSelectedValues = 0u;
    do {
        restoreDataChunkSelectorState();
        if (rightDataChunk->state->selectedSize == rightDataChunk->state->currIdx + 1ul) {
            rightDataChunk->state->currIdx = -1;
            prevOperator->getNextTuples();
            sortSelectedPos(leftNodeIDVector);
            sortSelectedPos(rightNodeIDVector);
            leftIdx = 0;
            if (leftDataChunk->state->selectedSize == 0) {
                break;
            }
        }
        // Flatten right dataChunk
        rightDataChunk->state->currIdx++;
        saveDataChunkSelectorState();
        auto rightPos = rightDataChunk->state->getPositionOfCurrIdx();
        auto rightNodeOffset = rightNodeIDVector->readNodeOffset(rightPos);
        numSelectedValues = 0u;
        while (leftIdx < leftDataChunk->state->selectedSize) {
            auto leftPos = leftDataChunk->state->selectedPositions[leftIdx];
            auto leftNodeOffset = leftNodeIDVector->readNodeOffset(leftPos);
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
}

} // namespace processor
} // namespace graphflow
