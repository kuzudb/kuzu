#include "src/processor/include/operator/list_reader/rel_property_list_reader.h"

namespace graphflow {
namespace processor {

void RelPropertyListReader::initialize(Graph* graph) {
    ListReader::initialize(graph);
    lists = graph->getRelPropertyLists(direction, nodeLabel, relLabel, propertyName);
    auto name =
        direction == Direction::FWD ?
            "(" + boundVariableName + "->" + extensionVariableName + ")" + "." + propertyName :
            "(" + boundVariableName + "<-" + extensionVariableName + ")" + "." + propertyName;
    outValueVector = make_shared<ValueVector>(name, 8 /* placeholder */);
    inDataChunk->append(outValueVector);
}

void RelPropertyListReader::getNextTuples() {
    lists->reclaim(handle);
    prevOperator->getNextTuples();
    if (inDataChunk->size > 0) {
        nodeID_t nodeID;
        inNodeIDVector->readValue(inDataChunk->curr_idx, nodeID);
        lists->readValues(nodeID, outValueVector, inDataChunk->size, handle);
    }
}

} // namespace processor
} // namespace graphflow
