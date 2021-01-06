#include "src/processor/include/operator/logical/property_reader/logical_node_property_reader.h"

#include "src/processor/include/operator/logical/logical_operator_ser_deser.h"
#include "src/processor/include/operator/physical/column_reader/node_property_column_reader.h"

namespace graphflow {
namespace processor {

LogicalNodePropertyReader::LogicalNodePropertyReader(FileDeserHelper& fdsh)
    : nodeVarName(*fdsh.readString()), nodeLabel(*fdsh.readString()),
      propertyName(*fdsh.readString()) {
    this->setPrevOperator(deserializeOperator(fdsh));
}

unique_ptr<Operator> LogicalNodePropertyReader::mapToPhysical(
    const Graph& graph, VarToChunkAndVectorIdxMap& schema) {
    auto prevOperator = this->prevOperator->mapToPhysical(graph, schema);
    auto inDataChunkIdx = schema.getDataChunkPos(nodeVarName);
    auto inValueVectorIdx = schema.getValueVectorPos(nodeVarName);
    auto catalog = graph.getCatalog();
    auto label = catalog.getNodeLabelFromString(nodeLabel);
    return make_unique<NodePropertyColumnReader>(inDataChunkIdx, inValueVectorIdx,
        graph.getNodePropertyColumn(label, propertyName), move(prevOperator));
}

void LogicalNodePropertyReader::serialize(FileSerHelper& fsh) {
    fsh.writeString(typeid(LogicalNodePropertyReader).name());
    fsh.writeString(nodeVarName);
    fsh.writeString(nodeLabel);
    fsh.writeString(propertyName);
    LogicalOperator::serialize(fsh);
}

} // namespace processor
} // namespace graphflow
