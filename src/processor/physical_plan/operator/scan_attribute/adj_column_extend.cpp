#include "src/processor/include/physical_plan/operator/scan_attribute/adj_column_extend.h"

namespace graphflow {
namespace processor {

AdjColumnExtend::AdjColumnExtend(uint32_t inAndOutDataChunkPos, uint32_t inValueVectorPos,
    uint32_t outValueVectorPos, Column* column, unique_ptr<PhysicalOperator> prevOperator,
    ExecutionContext& context, uint32_t id)
    : ScanColumn{inAndOutDataChunkPos, inValueVectorPos, outValueVectorPos, column,
          move(prevOperator), context, id},
      FilteringOperator(this->inDataChunk) {
    outValueVector = make_shared<ValueVector>(context.memoryManager, NODE);
    inDataChunk->insert(outValueVectorPos, outValueVector);
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
