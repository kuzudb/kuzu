
#include "src/processor/include/operator/property_reader/property_reader.h"

namespace graphflow {
namespace processor {

void PropertyReader::initialize(Graph* graph, shared_ptr<MorselDesc>& morsel) {
    prevOperator->initialize(graph, morsel);
    outDataChunks = prevOperator->getOutDataChunks();
    outDataChunk = outDataChunks[dataChunkIdx];
    nodeIDVector =
        static_pointer_cast<BaseNodeIDVector>(outDataChunk->valueVectors[nodeIDVectorIdx]);
}

void PropertyReader::getNextTuples() {
    column->reclaim(handle);
    prevOperator->getNextTuples();
    if (outDataChunk->size > 0) {
        column->readValues(nodeIDVector, outValueVector, outDataChunk->size, handle);
    }
}

} // namespace processor
} // namespace graphflow
