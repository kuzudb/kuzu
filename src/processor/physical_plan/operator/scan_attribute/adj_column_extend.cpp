#include "src/processor/include/physical_plan/operator/scan_attribute/adj_column_extend.h"

namespace graphflow {
namespace processor {

void AdjColumnExtend::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    ScanColumn::initResultSet(resultSet);
    outValueVector = make_shared<ValueVector>(context.memoryManager, NODE);
    inDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
}

void AdjColumnExtend::reInitialize() {
    PhysicalOperator::reInitialize();
    FilteringOperator::reInitialize();
}

void AdjColumnExtend::getNextTuples() {
    metrics->executionTime.start();
    bool hasAtLeastOneNonNullValue;
    do {
        restoreDataChunkSelectorState(inDataChunk);
        ScanColumn::getNextTuples();
        saveDataChunkSelectorState(inDataChunk);
        hasAtLeastOneNonNullValue = outValueVector->discardNullNodes();
    } while (inDataChunk->state->selectedSize > 0 && !hasAtLeastOneNonNullValue);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(inDataChunk->state->selectedSize);
}

} // namespace processor
} // namespace graphflow
