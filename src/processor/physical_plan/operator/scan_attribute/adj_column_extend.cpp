#include "src/processor/include/physical_plan/operator/scan_attribute/adj_column_extend.h"

namespace graphflow {
namespace processor {

void AdjColumnExtend::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    ScanAttribute::initResultSet(resultSet);
    outValueVector = make_shared<ValueVector>(context.memoryManager, NODE);
    inDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
}

void AdjColumnExtend::reInitialize() {
    PhysicalOperator::reInitialize();
    FilteringOperator::reInitialize();
}

bool AdjColumnExtend::getNextTuples() {
    metrics->executionTime.start();
    bool hasAtLeastOneNonNullValue;
    do {
        restoreDataChunkSelectorState(inDataChunk);
        if (!prevOperator->getNextTuples()) {
            metrics->executionTime.stop();
            return false;
        }
        saveDataChunkSelectorState(inDataChunk);
        outValueVector->setAllNull();
        column->readValues(inValueVector, outValueVector, *metrics->bufferManagerMetrics);
        hasAtLeastOneNonNullValue = outValueVector->discardNullNodes();
    } while (!hasAtLeastOneNonNullValue);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(inDataChunk->state->selectedSize);
    return true;
}

} // namespace processor
} // namespace graphflow
