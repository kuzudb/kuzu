#include "src/processor/include/physical_plan/operator/scan_attribute/adj_column_extend.h"

namespace graphflow {
namespace processor {

AdjColumnExtend::AdjColumnExtend(uint64_t dataChunkPos, uint64_t valueVectorPos, BaseColumn* column,
    unique_ptr<PhysicalOperator> prevOperator)
    : ScanStructuredColumn{dataChunkPos, valueVectorPos, column, move(prevOperator)},
      prevInNumSelectedValues(0ul) {
    auto outNodeIDVector = make_shared<NodeIDVector>(
        0, ((AdjColumn*)column)->getCompressionScheme(), false /*inNodeIDVector->isSequence()*/);
    outValueVector = static_pointer_cast<ValueVector>(outNodeIDVector);
    inDataChunk->append(outValueVector);
}

void AdjColumnExtend::getNextTuples() {
    bool hasAtLeastOneNonNullValue;
    do {
        inDataChunk->state->numSelectedValues = prevInNumSelectedValues;
        ScanStructuredColumn::getNextTuples();
        prevInNumSelectedValues = inDataChunk->state->numSelectedValues;
        hasAtLeastOneNonNullValue =
            static_pointer_cast<NodeIDVector>(outValueVector)->discardNulls();
    } while (inDataChunk->state->size > 0 && !hasAtLeastOneNonNullValue);
}

} // namespace processor
} // namespace graphflow
