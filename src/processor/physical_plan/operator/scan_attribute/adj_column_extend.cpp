#include "src/processor/include/physical_plan/operator/scan_attribute/adj_column_extend.h"

namespace graphflow {
namespace processor {

AdjColumnExtend::AdjColumnExtend(uint64_t dataChunkPos, uint64_t valueVectorPos, Column* column,
    unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
    : ScanColumn{dataChunkPos, valueVectorPos, column, move(prevOperator), context, id},
      FilteringOperator(this->inDataChunk) {
    outValueVector = make_shared<ValueVector>(context.memoryManager, NODE);
    inDataChunk->append(outValueVector);
}

void AdjColumnExtend::reInitialize() {
    PhysicalOperator::reInitialize();
    FilteringOperator::reInitialize();
}

void AdjColumnExtend::getNextTuples() {
    metrics->executionTime.start();
    bool hasAtLeastOneNonNullValue;
    do {
        restoreDataChunkSelectorState();
        ScanColumn::getNextTuples();
        saveDataChunkSelectorState();
        hasAtLeastOneNonNullValue = outValueVector->discardNullNodes();
    } while (inDataChunk->state->selectedSize > 0 && !hasAtLeastOneNonNullValue);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(inDataChunk->state->selectedSize);
}

} // namespace processor
} // namespace graphflow
