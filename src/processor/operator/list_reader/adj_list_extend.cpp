#include "src/processor/include/operator/list_reader/adj_list_extend.h"

namespace graphflow {
namespace processor {

void AdjListExtend::initialize(Graph* graph) {
    ListReader::initialize(graph);
    outNodeIDVector = make_shared<NodeIDVector>(extensionVariableName, NodeIDCompressionScheme());
    outDataChunk = make_shared<DataChunk>();
    outDataChunk->append(outNodeIDVector);
    dataChunks->append(outDataChunk);
    lists = graph->getAdjLists(direction, nodeLabel, relLabel);
}

bool AdjListExtend::hasNextMorsel() {
    return (inDataChunk->size > 0 && inDataChunk->size > inDataChunk->curr_idx + 1) ||
           prevOperator->hasNextMorsel();
}

void AdjListExtend::getNextTuples() {
    lists->reclaim(handle);
    if (inDataChunk->size == 0 || inDataChunk->size == inDataChunk->curr_idx) {
        inDataChunk->curr_idx = 0;
        prevOperator->getNextTuples();
    } else {
        inDataChunk->curr_idx++;
    }
    if (inDataChunk->size > 0) {
        nodeID_t nodeID;
        inNodeIDVector->readValue(inDataChunk->curr_idx, nodeID);
        lists->readValues(nodeID, outNodeIDVector, outDataChunk->size, handle);
    } else {
        outDataChunk->size = 0;
    }
}

} // namespace processor
} // namespace graphflow
