#include "src/processor/include/physical_plan/operator/column_reader/column_reader.h"

namespace graphflow {
namespace processor {

ColumnReader::ColumnReader(const uint64_t& dataChunkPos, const uint64_t& valueVectorPos,
    BaseColumn* column, unique_ptr<PhysicalOperator> prevOperator)
    : PhysicalOperator{move(prevOperator)}, dataChunkPos{dataChunkPos},
      valueVectorPos{valueVectorPos}, column{column} {
    dataChunks = this->prevOperator->getDataChunks();
    inDataChunk = dataChunks->getDataChunk(dataChunkPos);
    inNodeIDVector = static_pointer_cast<NodeIDVector>(inDataChunk->getValueVector(valueVectorPos));
    handle = make_unique<ColumnOrListsHandle>();
}

void ColumnReader::getNextTuples() {
    prevOperator->getNextTuples();
    if (inDataChunk->size > 0) {
        column->reclaim(handle);
        column->readValues(inNodeIDVector, outValueVector, inDataChunk->size, handle);
    }
}

} // namespace processor
} // namespace graphflow
