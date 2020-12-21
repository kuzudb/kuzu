
#include "src/processor/include/operator/column_reader/column_reader.h"

namespace graphflow {
namespace processor {

void ColumnReader::initialize(Graph* graph, shared_ptr<MorselDesc>& morsel) {
    prevOperator->initialize(graph, morsel);
    outDataChunks = prevOperator->getOutDataChunks();
    outDataChunk = outDataChunks[dataChunkIdx];
    nodeIDVector = static_pointer_cast<NodeIDVector>(outDataChunk->valueVectors[nodeIDVectorIdx]);
}

void ColumnReader::getNextTuples() {
    column->reclaim(handle);
    prevOperator->getNextTuples();
    if (outDataChunk->size > 0) {
        column->readValues(nodeIDVector, outValueVector, outDataChunk->size, handle);
    }
}

} // namespace processor
} // namespace graphflow
