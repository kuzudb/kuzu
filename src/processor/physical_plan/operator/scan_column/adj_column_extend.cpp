#include "src/processor/include/physical_plan/operator/scan_column/adj_column_extend.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> AdjColumnExtend::initResultSet() {
    BaseScanColumn::initResultSet();
    outputVector = make_shared<ValueVector>(context.memoryManager, NODE);
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
        hasAtLeastOneNonNullValue = outputVector->discardNullNodes();
    } while (!hasAtLeastOneNonNullValue);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(inputNodeIDDataChunk->state->selectedSize);
    return true;
}

} // namespace processor
} // namespace graphflow
