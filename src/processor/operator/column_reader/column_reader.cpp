
#include "src/processor/include/operator/column_reader/column_reader.h"

namespace graphflow {
namespace processor {

ColumnReader::ColumnReader(FileDeserHelper& fdsh)
    : nodeOrRelVarName{*fdsh.readString()}, nodeLabel{fdsh.read<label_t>()} {};

void ColumnReader::initialize(Graph* graph) {
    prevOperator->initialize(graph);
    dataChunks = prevOperator->getOutDataChunks();
    inNodeIDVector = static_pointer_cast<NodeIDVector>(
        dataChunks->getValueVectorAndSetDataChunk(nodeOrRelVarName, inDataChunk));
}

void ColumnReader::getNextTuples() {
    column->reclaim(handle);
    prevOperator->getNextTuples();
    if (inDataChunk->size > 0) {
        column->readValues(inNodeIDVector, outValueVector, inDataChunk->size, handle);
    }
}

void ColumnReader::serialize(FileSerHelper& fsh) {
    fsh.writeString(nodeOrRelVarName);
    fsh.write(nodeLabel);
}

} // namespace processor
} // namespace graphflow
