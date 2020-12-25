#include "src/processor/include/operator/extend/extend.h"

namespace graphflow {
namespace processor {

void Extend::initialize(Graph* graph, shared_ptr<MorselDesc>& morsel) {
    prevOperator->initialize(graph, morsel);
    dataChunks = prevOperator->getOutDataChunks();
    inNodeIDVector = static_pointer_cast<NodeIDVector>(
        dataChunks->getValueVectorAndSetDataChunk(boundVariableName, inDataChunk));
    outNodeIDVector = make_shared<NodeIDVector>(extensionVariableName, NodeIDCompressionScheme());
    outDataChunk = make_shared<DataChunk>();
    outDataChunk->append(outNodeIDVector);
    dataChunks->append(outDataChunk);
    adjLists = graph->getAdjLists(direction, nodeLabel, relLabel);
}

bool Extend::hasNextMorsel() {
    return (inDataChunk->size > 0 && inDataChunk->size > inDataChunk->curr_idx + 1) ||
           prevOperator->hasNextMorsel();
}

void Extend::getNextTuples() {
    adjLists->reclaim(handle);
    if (inDataChunk->size == 0 || inDataChunk->size == inDataChunk->curr_idx) {
        inDataChunk->curr_idx = 0;
        prevOperator->getNextTuples();
    } else {
        inDataChunk->curr_idx++;
    }
    if (inDataChunk->size > 0) {
        nodeID_t nodeID;
        inNodeIDVector->readValue(inDataChunk->curr_idx, nodeID);
        adjLists->readValues(nodeID, outNodeIDVector, outDataChunk->size, handle);
    } else {
        outDataChunk->size = 0;
    }
}

void Extend::cleanup() {
    adjLists->reclaim(handle);
    Operator::cleanup();
}

} // namespace processor
} // namespace graphflow
