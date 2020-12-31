
#include "src/processor/include/operator/column_reader/column_reader.h"

namespace graphflow {
namespace processor {

void ColumnReader::initialize(Graph* graph) {
    prevOperator->initialize(graph);
    dataChunks = prevOperator->getOutDataChunks();
    inNodeIDVector = static_pointer_cast<NodeIDVector>(
        dataChunks->getValueVectorAndSetDataChunk(boundVariableOrRelName, inDataChunk));
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
