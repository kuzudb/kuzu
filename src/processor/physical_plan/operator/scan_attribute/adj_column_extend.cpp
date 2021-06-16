#include "src/processor/include/physical_plan/operator/scan_attribute/adj_column_extend.h"

namespace graphflow {
namespace processor {

AdjColumnExtend::AdjColumnExtend(uint64_t dataChunkPos, uint64_t valueVectorPos, BaseColumn* column,
    unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
    : ScanColumn{dataChunkPos, valueVectorPos, column, move(prevOperator), context, id},
      prevInNumSelectedValues(0ul) {
    auto outNodeIDVector = make_shared<NodeIDVector>(
        0, ((AdjColumn*)column)->getCompressionScheme(), false /*inNodeIDVector->isSequence()*/);
    outValueVector = static_pointer_cast<ValueVector>(outNodeIDVector);
    inDataChunk->append(outValueVector);
}

void AdjColumnExtend::getNextTuples() {
    metrics->executionTime.start();
    bool hasAtLeastOneNonNullValue;
    do {
        inDataChunk->state->size = prevInNumSelectedValues;
        ScanColumn::getNextTuples();
        prevInNumSelectedValues = inDataChunk->state->size;
        hasAtLeastOneNonNullValue =
            static_pointer_cast<NodeIDVector>(outValueVector)->discardNulls();
    } while (inDataChunk->state->size > 0 && !hasAtLeastOneNonNullValue);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(inDataChunk->state->size);
}

} // namespace processor
} // namespace graphflow
