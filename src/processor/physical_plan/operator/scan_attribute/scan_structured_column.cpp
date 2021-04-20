#include "src/processor/include/physical_plan/operator/scan_attribute/scan_structured_column.h"

namespace graphflow {
namespace processor {

ScanStructuredColumn::ScanStructuredColumn(uint64_t dataChunkPos, uint64_t valueVectorPos,
    BaseColumn* column, unique_ptr<PhysicalOperator> prevOperator)
    : ScanAttribute{dataChunkPos, valueVectorPos, move(prevOperator)}, column{column} {
    dataChunks = this->prevOperator->getDataChunks();
    inDataChunk = dataChunks->getDataChunk(dataChunkPos);
    inNodeIDVector = static_pointer_cast<NodeIDVector>(inDataChunk->getValueVector(valueVectorPos));
    handle = make_unique<ColumnOrListsHandle>();
}

void ScanStructuredColumn::getNextTuples() {
    do {
        prevOperator->getNextTuples();
    } while (inDataChunk->state->size > 0 && inDataChunk->state->numSelectedValues == 0);
    if (inDataChunk->state->size > 0) {
        column->reclaim(handle);
        column->readValues(inNodeIDVector, outValueVector, handle);
    }
}

} // namespace processor
} // namespace graphflow
