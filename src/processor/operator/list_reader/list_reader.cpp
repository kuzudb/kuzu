#include "src/processor/include/operator/list_reader/list_reader.h"

namespace graphflow {
namespace processor {

ListReader::ListReader(FileDeserHelper& fdsh)
    : boundNodeVarName{*fdsh.readString()},
      nbrNodeVarName{*fdsh.readString()}, direction{fdsh.read<Direction>()},
      nodeLabel{fdsh.read<label_t>()}, relLabel{fdsh.read<label_t>()} {};

void ListReader::initialize(Graph* graph) {
    prevOperator->initialize(graph);
    dataChunks = prevOperator->getOutDataChunks();
    inNodeIDVector = static_pointer_cast<NodeIDVector>(
        dataChunks->getValueVectorAndSetDataChunk(boundNodeVarName, inDataChunk));
}

void ListReader::cleanup() {
    lists->reclaim(handle);
    Operator::cleanup();
}

void ListReader::serialize(FileSerHelper& fsh) {
    fsh.writeString(boundNodeVarName);
    fsh.writeString(nbrNodeVarName);
    fsh.write(direction);
    fsh.write(nodeLabel);
    fsh.write(relLabel);
}

} // namespace processor
} // namespace graphflow
