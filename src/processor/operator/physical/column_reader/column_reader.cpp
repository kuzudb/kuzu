#include "src/processor/include/operator/physical/column_reader/column_reader.h"

namespace graphflow {
namespace processor {

ColumnReader::ColumnReader(const uint64_t& inDataChunkIdx, const uint64_t& inValueVectorIdx,
    BaseColumn* column, unique_ptr<Operator> prevOperator)
    : Operator{move(prevOperator)}, inDataChunkIdx{inDataChunkIdx},
      inValueVectorIdx{inValueVectorIdx}, column{column} {
    dataChunks = this->prevOperator->getOutDataChunks();
    inDataChunk = dataChunks->getDataChunk(inDataChunkIdx);
    inNodeIDVector = static_pointer_cast<NodeIDVector>(
        dataChunks->getValueVector(inDataChunkIdx, inValueVectorIdx));
    handle = make_unique<VectorFrameHandle>();
}

void ColumnReader::cleanup() {
    column->reclaim(handle);
    prevOperator->cleanup();
}

void ColumnReader::getNextTuples() {
    column->reclaim(handle);
    prevOperator->getNextTuples();
    if (inDataChunk->size > 0) {
        column->readValues(inNodeIDVector, outValueVector, inDataChunk->size, handle);
    }
}

} // namespace processor
} // namespace graphflow
