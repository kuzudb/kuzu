
#include "src/processor/include/operator/column_reader/column_reader.h"

namespace graphflow {
namespace processor {

void ColumnReader::initialize(Graph* graph, shared_ptr<MorselDesc>& morsel) {
    prevOperator->initialize(graph, morsel);
    dataChunks = prevOperator->getOutDataChunks();
    inNodeIDVector = static_pointer_cast<NodeIDVector>(
        dataChunks->getValueVectorAndSetDataChunk(boundVariableOrRelName, dataChunk));
}

void ColumnReader::getNextTuples() {
    column->reclaim(handle);
    prevOperator->getNextTuples();
    if (dataChunk->size > 0) {
        column->readValues(inNodeIDVector, outValueVector, dataChunk->size, handle);
    }
}

} // namespace processor
} // namespace graphflow
