#include "src/processor/include/physical_plan/operator/scan_column/adj_column_extend.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> AdjColumnExtend::init(ExecutionContext* context) {
    resultSet = BaseScanColumn::init(context);
    outputVector = make_shared<ValueVector>(context->memoryManager, NODE_ID);
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
        nodeIDColumn->read(transaction, inputNodeIDVector, outputVector);
        hasAtLeastOneNonNullValue = discardNullNodesInVector(*outputVector);
    } while (!hasAtLeastOneNonNullValue);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(inputNodeIDDataChunk->state->selectedSize);
    return true;
}

bool AdjColumnExtend::discardNullNodesInVector(ValueVector& valueVector) {
    assert(valueVector.dataType.typeID == NODE_ID);
    if (valueVector.state->isFlat()) {
        return !valueVector.isNull(valueVector.state->getPositionOfCurrIdx());
    } else {
        NodeIDVector::discardNull(valueVector);
        return valueVector.state->selectedSize > 0;
    }
}

} // namespace processor
} // namespace graphflow
