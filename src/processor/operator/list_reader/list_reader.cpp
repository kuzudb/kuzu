#include "src/processor/include/operator/list_reader/list_reader.h"

namespace graphflow {
namespace processor {

void ListReader::initialize(Graph* graph) {
    prevOperator->initialize(graph);
    dataChunks = prevOperator->getOutDataChunks();
    inNodeIDVector = static_pointer_cast<NodeIDVector>(
        dataChunks->getValueVectorAndSetDataChunk(boundVariableName, inDataChunk));
}

void ListReader::cleanup() {
    lists->reclaim(handle);
    Operator::cleanup();
}

} // namespace processor
} // namespace graphflow
