#include "src/processor/include/operator/list_reader/rel_property_list_reader.h"

#include "src/processor/include/operator/operator_ser_deser_factory.h"

namespace graphflow {
namespace processor {

RelPropertyListReader::RelPropertyListReader(FileDeserHelper& fdsh)
    : ListReader{fdsh}, propertyName{*fdsh.readString()} {
    this->setPrevOperator(deserializeOperator(fdsh));
}

void RelPropertyListReader::initialize(Graph* graph) {
    ListReader::initialize(graph);
    lists = graph->getRelPropertyLists(direction, nodeLabel, relLabel, propertyName);
    auto name = direction == Direction::FWD ?
                    "(" + boundNodeVarName + "->" + nbrNodeVarName + ")" + "." + propertyName :
                    "(" + boundNodeVarName + "<-" + nbrNodeVarName + ")" + "." + propertyName;
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

void RelPropertyListReader::serialize(FileSerHelper& fsh) {
    string typeIDStr = typeid(RelPropertyListReader).name();
    fsh.writeString(typeIDStr);
    ListReader::serialize(fsh);
    fsh.writeString(propertyName);
    Operator::serialize(fsh);
}

} // namespace processor
} // namespace graphflow
