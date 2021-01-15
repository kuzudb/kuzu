#include "src/processor/include/operator/logical/property_reader/logical_rel_property_reader.h"

#include "src/processor/include/operator/logical/logical_operator_ser_deser.h"
#include "src/processor/include/operator/physical/column_reader/rel_property_column_reader.h"
#include "src/processor/include/operator/physical/list_reader/rel_property_list_reader.h"

namespace graphflow {
namespace processor {

LogicalRelPropertyReader::LogicalRelPropertyReader(FileDeserHelper& fdsh)
    : srcNodeVarName{*fdsh.readString()}, srcNodeVarLabel{*fdsh.readString()},
      dstNodeVarName{*fdsh.readString()}, dstNodeVarLabel{*fdsh.readString()},
      relLabel{*fdsh.readString()}, extensionDirectionInPlan{fdsh.read<Direction>()},
      propertyName(*fdsh.readString()) {
    this->setPrevOperator(deserializeOperator(fdsh));
}

unique_ptr<Operator> LogicalRelPropertyReader::mapToPhysical(
    const Graph& graph, VarToChunkAndVectorIdxMap& schema) {
    auto prevOperator = this->prevOperator->mapToPhysical(graph, schema);
    auto& catalog = graph.getCatalog();
    auto label = catalog.getRelLabelFromString(relLabel.c_str());
    string inStrNodeVarName;
    string inStrNodeLabel;
    string outStrNodeVarName;
    if (catalog.isSingleCaridinalityInDir(label, FWD)) {
        inStrNodeVarName = srcNodeVarName;
        inStrNodeLabel = srcNodeVarLabel;
        outStrNodeVarName = inStrNodeVarName;
    } else if (catalog.isSingleCaridinalityInDir(label, BWD)) {
        inStrNodeVarName = dstNodeVarName;
        inStrNodeLabel = dstNodeVarLabel;
        outStrNodeVarName = inStrNodeVarName;
    } else {
        inStrNodeVarName = extensionDirectionInPlan == FWD ? srcNodeVarName : dstNodeVarName;
        inStrNodeLabel = extensionDirectionInPlan == FWD ? srcNodeVarLabel : dstNodeVarLabel;
        outStrNodeVarName = extensionDirectionInPlan == FWD ? dstNodeVarName : srcNodeVarName;
    }
    auto inDataChunkPos = schema.getDataChunkPos(inStrNodeVarName);
    auto inValueVectorPos = schema.getValueVectorPos(inStrNodeVarName);
    auto outDataChunkPos = schema.getDataChunkPos(outStrNodeVarName);
    auto nodeLabel = catalog.getNodeLabelFromString(inStrNodeLabel.c_str());
    auto property = catalog.getRelPropertyKeyFromString(label, propertyName);
    auto& relsStore = graph.getRelsStore();
    if (catalog.isSingleCaridinalityInDir(label, extensionDirectionInPlan)) {
        auto column = relsStore.getRelPropertyColumn(label, nodeLabel, property);
        return make_unique<RelPropertyColumnReader>(
            inDataChunkPos, inValueVectorPos, column, move(prevOperator));
    } else {
        auto lists =
            relsStore.getRelPropertyLists(extensionDirectionInPlan, nodeLabel, label, property);
        return make_unique<RelPropertyListReader>(
            inDataChunkPos, inValueVectorPos, outDataChunkPos, lists, move(prevOperator));
    }
}

void LogicalRelPropertyReader::serialize(FileSerHelper& fsh) {
    fsh.writeString(typeid(LogicalRelPropertyReader).name());
    fsh.writeString(srcNodeVarName);
    fsh.writeString(srcNodeVarLabel);
    fsh.writeString(dstNodeVarName);
    fsh.writeString(dstNodeVarLabel);
    fsh.writeString(relLabel);
    fsh.write(extensionDirectionInPlan);
    fsh.writeString(propertyName);
    LogicalOperator::serialize(fsh);
}

} // namespace processor
} // namespace graphflow
