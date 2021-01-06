#include "src/processor/include/operator/logical/property_reader/logical_rel_property_reader.h"

#include "src/processor/include/operator/logical/logical_operator_ser_deser.h"
#include "src/processor/include/operator/physical/column_reader/rel_property_column_reader.h"
#include "src/processor/include/operator/physical/list_reader/rel_property_list_reader.h"

namespace graphflow {
namespace processor {

LogicalRelPropertyReader::LogicalRelPropertyReader(FileDeserHelper& fdsh)
    : fromNodeVarName{*fdsh.readString()}, toNodeVarName{*fdsh.readString()},
      toNodeVarLabel{*fdsh.readString()}, relLabel{*fdsh.readString()},
      direction{fdsh.read<Direction>()}, propertyName{*fdsh.readString()} {
    this->setPrevOperator(deserializeOperator(fdsh));
}

unique_ptr<Operator> LogicalRelPropertyReader::mapToPhysical(
    const Graph& graph, VarToChunkAndVectorIdxMap& schema) {
    auto prevOperator = this->prevOperator->mapToPhysical(graph, schema);
    auto inDataChunkIdx = schema.getDataChunkPos(fromNodeVarName);
    auto inValueVectorIdx = schema.getValueVectorPos(fromNodeVarName);
    auto catalog = graph.getCatalog();
    auto label = catalog.getRelLabelFromString(relLabel);
    if (catalog.isSingleCaridinalityInDir(label, direction)) {
        auto nodeLabel = catalog.getNodeLabelFromString(fromNodeVarLabel);
        auto column = graph.getRelPropertyColumn(label, nodeLabel, propertyName);
        return make_unique<RelPropertyColumnReader>(
            inDataChunkIdx, inValueVectorIdx, column, move(prevOperator));
    } else {
        auto nodeLabel = catalog.getNodeLabelFromString(toNodeVarLabel);
        auto lists = graph.getRelPropertyLists(direction, label, nodeLabel, propertyName);
        return make_unique<RelPropertyListReader>(
            inDataChunkIdx, inValueVectorIdx, lists, move(prevOperator));
    }
}

void LogicalRelPropertyReader::serialize(FileSerHelper& fsh) {
    fsh.writeString(typeid(LogicalRelPropertyReader).name());
    fsh.writeString(fromNodeVarName);
    fsh.writeString(fromNodeVarLabel);
    fsh.writeString(toNodeVarName);
    fsh.writeString(toNodeVarLabel);
    fsh.writeString(relLabel);
    fsh.write(direction);
    fsh.writeString(propertyName);
    LogicalOperator::serialize(fsh);
}

} // namespace processor
} // namespace graphflow
