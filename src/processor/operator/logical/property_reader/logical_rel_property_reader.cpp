#include "src/processor/include/operator/logical/property_reader/logical_rel_property_reader.h"

#include "src/processor/include/operator/logical/logical_operator_ser_deser.h"
#include "src/processor/include/operator/physical/column_reader/rel_property_column_reader.h"
#include "src/processor/include/operator/physical/list_reader/rel_property_list_reader.h"

namespace graphflow {
namespace processor {

LogicalRelPropertyReader::LogicalRelPropertyReader(FileDeserHelper& fdsh)
    : boundNodeVarName(*fdsh.readString()), boundNodeVarLabel(*fdsh.readString()),
      nbrNodeVarName(*fdsh.readString()), nbrNodeVarLabel(*fdsh.readString()),
      relLabel(*fdsh.readString()), direction(fdsh.read<Direction>()),
      propertyName(*fdsh.readString()) {
    this->setPrevOperator(deserializeOperator(fdsh));
}

unique_ptr<Operator> LogicalRelPropertyReader::mapToPhysical(
    const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto prevOperator = this->prevOperator->mapToPhysical(graph, physicalOperatorInfo);
    auto& catalog = graph.getCatalog();
    auto label = catalog.getRelLabelFromString(relLabel.c_str());
    auto inDataChunkPos = physicalOperatorInfo.getDataChunkPos(boundNodeVarName);
    auto inValueVectorPos = physicalOperatorInfo.getValueVectorPos(boundNodeVarName);
    auto outDataChunkPos = physicalOperatorInfo.getDataChunkPos(nbrNodeVarName);
    auto nodeLabel = catalog.getNodeLabelFromString(boundNodeVarLabel.c_str());
    auto property = catalog.getRelPropertyKeyFromString(label, propertyName);
    auto& relsStore = graph.getRelsStore();
    if (catalog.isSingleCaridinalityInDir(label, direction)) {
        auto column = relsStore.getRelPropertyColumn(label, nodeLabel, property);
        return make_unique<RelPropertyColumnReader>(
            inDataChunkPos, inValueVectorPos, column, move(prevOperator));
    } else {
        auto lists = relsStore.getRelPropertyLists(direction, nodeLabel, label, property);
        return make_unique<RelPropertyListReader>(
            inDataChunkPos, inValueVectorPos, outDataChunkPos, lists, move(prevOperator));
    }
}

void LogicalRelPropertyReader::serialize(FileSerHelper& fsh) {
    fsh.writeString(typeid(LogicalRelPropertyReader).name());
    fsh.writeString(boundNodeVarName);
    fsh.writeString(boundNodeVarLabel);
    fsh.writeString(nbrNodeVarName);
    fsh.writeString(nbrNodeVarLabel);
    fsh.writeString(relLabel);
    fsh.write(direction);
    fsh.writeString(propertyName);
    LogicalOperator::serialize(fsh);
}

} // namespace processor
} // namespace graphflow
