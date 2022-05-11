#include "src/processor/include/physical_plan/operator/scan_column/adj_column_extend.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> AdjColumnExtend::init(ExecutionContext* context) {
    resultSet = BaseScanColumn::init(context);
    outputVector = make_shared<ValueVector>(context->memoryManager, NODE);
    inputNodeIDDataChunk->insert(outputVectorPos.valueVectorPos, outputVector);
    return resultSet;
}

void AdjColumnExtend::reInitToRerunSubPlan() {
    BaseScanColumn::reInitToRerunSubPlan();
    FilteringOperator::reInitToRerunSubPlan();
}

bool AdjColumnExtend::getNextTuples() {
    metrics->executionTime.start();
    bool hasAtLeastOneNonNullValue;
    do {
        restoreDataChunkSelectorState(inputNodeIDDataChunk);
        if (!children[0]->getNextTuples()) {
            metrics->executionTime.stop();
            return false;
        }
        saveDataChunkSelectorState(inputNodeIDDataChunk);
        outputVector->setAllNull();
        nodeIDColumn->readValues(inputNodeIDVector, outputVector);
        hasAtLeastOneNonNullValue = discardNullNodesInVector(*outputVector);
    } while (!hasAtLeastOneNonNullValue);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(inputNodeIDDataChunk->state->selectedSize);
    return true;
}

bool AdjColumnExtend::discardNullNodesInVector(ValueVector& valueVector) {
    assert(valueVector.dataType.typeID == NODE);
    if (valueVector.state->isFlat()) {
        return !valueVector.isNull(valueVector.state->getPositionOfCurrIdx());
    } else {
        auto selectedPos = 0u;
        if (valueVector.state->isUnfiltered()) {
            valueVector.state->resetSelectorToValuePosBuffer();
            for (auto i = 0u; i < valueVector.state->selectedSize; i++) {
                valueVector.state->selectedPositions[selectedPos] = i;
                selectedPos += !valueVector.isNull(i);
            }
        } else {
            for (auto i = 0u; i < valueVector.state->selectedSize; i++) {
                auto pos = valueVector.state->selectedPositions[i];
                valueVector.state->selectedPositions[selectedPos] = i;
                selectedPos += !valueVector.isNull(pos);
            }
        }
        valueVector.state->selectedSize = selectedPos;
        return valueVector.state->selectedSize > 0;
    }
}

} // namespace processor
} // namespace graphflow
