#include "src/processor/include/physical_plan/operator/scan_column/scan_column.h"

namespace graphflow {
namespace processor {

ScanColumn::ScanColumn(uint64_t dataChunkPos, uint64_t valueVectorPos, BaseColumn* column,
    unique_ptr<PhysicalOperator> prevOperator)
    : PhysicalOperator{move(prevOperator), SCAN_COLUMN}, dataChunkPos{dataChunkPos},
      valueVectorPos{valueVectorPos}, column{column} {
    dataChunks = this->prevOperator->getDataChunks();
    inDataChunk = dataChunks->getDataChunk(dataChunkPos);
    inNodeIDVector = static_pointer_cast<NodeIDVector>(inDataChunk->getValueVector(valueVectorPos));
    handle = make_unique<ColumnOrListsHandle>();
}

void ScanColumn::getNextTuples() {
    do {
        prevOperator->getNextTuples();
    } while (inDataChunk->size > 0 && inDataChunk->numSelectedValues == 0);
    if (inDataChunk->size > 0) {
        column->reclaim(handle);
        column->readValues(inNodeIDVector, outValueVector, inDataChunk->size, handle);
    }
}

} // namespace processor
} // namespace graphflow
