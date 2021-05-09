#include "src/processor/include/physical_plan/operator/scan_attribute/scan_column.h"

namespace graphflow {
namespace processor {

ScanColumn::ScanColumn(uint64_t dataChunkPos, uint64_t valueVectorPos, BaseColumn* column,
    unique_ptr<PhysicalOperator> prevOperator)
    : ScanAttribute{dataChunkPos, valueVectorPos, move(prevOperator)}, column{column} {
    resultSet = this->prevOperator->getResultSet();
    inDataChunk = resultSet->dataChunks[dataChunkPos];
    inNodeIDVector = static_pointer_cast<NodeIDVector>(inDataChunk->getValueVector(valueVectorPos));
    handle = make_unique<DataStructureHandle>();
}

void ScanColumn::getNextTuples() {
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
