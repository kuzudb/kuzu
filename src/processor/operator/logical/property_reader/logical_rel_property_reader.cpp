#include "src/processor/include/operator/logical/property_reader/logical_rel_property_reader.h"

#include "src/processor/include/operator/logical/logical_operator_ser_deser.h"
#include "src/processor/include/operator/physical/column_reader/rel_property_column_reader.h"
#include "src/processor/include/operator/physical/list_reader/rel_property_list_reader.h"

namespace graphflow {
namespace processor {

LogicalRelPropertyReader::LogicalRelPropertyReader(FileDeserHelper& fdsh)
    : srcNodeVarName{*fdsh.readString()}, srcNodeVarLabel{*fdsh.readString()},
      dstNodeVarName{*fdsh.readString()}, dstNodeVarLabel{*fdsh.readString()},
      relLabel{*fdsh.readString()}, direction{fdsh.read<Direction>()},
      propertyName(*fdsh.readString()) {
    this->setPrevOperator(deserializeOperator(fdsh));
}

unique_ptr<Operator> LogicalRelPropertyReader::mapToPhysical(
    const Graph& graph, VarToChunkAndVectorIdxMap& schema) {
    auto prevOperator = this->prevOperator->mapToPhysical(graph, schema);
    auto& catalog = graph.getCatalog();
    auto label = catalog.getRelLabelFromString(relLabel.c_str());
    string strNodeVarName;
    string strNodeLabel;
    if (catalog.isSingleCaridinalityInDir(label, FWD)) {
        strNodeVarName = srcNodeVarName;
        strNodeLabel = srcNodeVarLabel;
    } else if (catalog.isSingleCaridinalityInDir(label, BWD)) {
        strNodeVarName = dstNodeVarName;
        strNodeLabel = dstNodeVarLabel;
    } else {
        strNodeVarName = direction == FWD ? srcNodeVarName : dstNodeVarName;
        strNodeLabel = direction == FWD ? srcNodeVarLabel : dstNodeVarLabel;
    }
    auto dataChunkPos = schema.getDataChunkPos(strNodeVarName);
    auto valueVectorPos = schema.getValueVectorPos(strNodeVarName);
    auto nodeLabel = catalog.getNodeLabelFromString(strNodeLabel.c_str());
    if (catalog.isSingleCaridinalityInDir(label, direction)) {
        auto column = graph.getRelPropertyColumn(label, nodeLabel, propertyName);
        return make_unique<RelPropertyColumnReader>(
            dataChunkPos, valueVectorPos, column, move(prevOperator));
    } else {
        auto lists = graph.getRelPropertyLists(direction, nodeLabel, label, propertyName);
        return make_unique<RelPropertyListReader>(
            dataChunkPos, valueVectorPos, lists, move(prevOperator));
    }
}

void LogicalRelPropertyReader::serialize(FileSerHelper& fsh) {
    fsh.writeString(typeid(LogicalRelPropertyReader).name());
    fsh.writeString(srcNodeVarName);
    fsh.writeString(srcNodeVarLabel);
    fsh.writeString(dstNodeVarName);
    fsh.writeString(dstNodeVarLabel);
    fsh.writeString(relLabel);
    fsh.write(direction);
    fsh.writeString(propertyName);
    LogicalOperator::serialize(fsh);
}

} // namespace processor
} // namespace graphflow
