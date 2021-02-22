#include "src/processor/include/operator/logical/extend/logical_extend.h"

#include "src/processor/include/operator/logical/logical_operator_ser_deser.h"
#include "src/processor/include/operator/physical/column_reader/adj_column_extend.h"
#include "src/processor/include/operator/physical/list_reader/extend/adj_list_flatten_and_extend.h"
#include "src/processor/include/operator/physical/list_reader/extend/adj_list_only_extend.h"

namespace graphflow {
namespace processor {

LogicalExtend::LogicalExtend(FileDeserHelper& fdsh)
    : boundNodeVarName(*fdsh.readString()), boundNodeVarLabel(*fdsh.readString()),
      nbrNodeVarName(*fdsh.readString()), nbrNodeVarLabel(*fdsh.readString()),
      relLabel(*fdsh.readString()), direction(fdsh.read<Direction>()) {
    this->setPrevOperator(deserializeOperator(fdsh));
}

unique_ptr<Operator> LogicalExtend::mapToPhysical(
    const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto prevOperator = this->prevOperator->mapToPhysical(graph, physicalOperatorInfo);
    auto dataChunkPos = physicalOperatorInfo.getDataChunkPos(boundNodeVarName);
    auto valueVectorPos = physicalOperatorInfo.getValueVectorPos(boundNodeVarName);
    auto& catalog = graph.getCatalog();
    auto relLabelFromString = catalog.getRelLabelFromString(relLabel.c_str());
    auto dataChunks = prevOperator->getDataChunks();
    auto nodeLabel = catalog.getNodeLabelFromString(boundNodeVarLabel.c_str());
    auto& relsStore = graph.getRelsStore();
    if (catalog.isSingleCaridinalityInDir(relLabelFromString, direction)) {
        physicalOperatorInfo.put(
            nbrNodeVarName, dataChunkPos, dataChunks->getNumValueVectors(dataChunkPos));
        return make_unique<AdjColumnExtend>(dataChunkPos, valueVectorPos,
            relsStore.getAdjColumn(direction, nodeLabel, relLabelFromString), move(prevOperator));
    } else {
        auto numChunks = dataChunks->getNumDataChunks();
        auto listSyncer = make_shared<ListSyncer>();
        physicalOperatorInfo.put(nbrNodeVarName, numChunks /*dataChunkPos*/, 0 /*valueVectorPos*/);
        physicalOperatorInfo.putListSyncer(numChunks, listSyncer);
        if (physicalOperatorInfo.isFlat(dataChunkPos)) {
            return make_unique<AdjListOnlyExtend>(dataChunkPos, valueVectorPos,
                relsStore.getAdjLists(direction, nodeLabel, relLabelFromString), listSyncer,
                move(prevOperator));
        } else {
            physicalOperatorInfo.setDataChunkAtPosAsFlat(dataChunkPos);
            return make_unique<AdjListFlattenAndExtend>(dataChunkPos, valueVectorPos,
                relsStore.getAdjLists(direction, nodeLabel, relLabelFromString), listSyncer,
                move(prevOperator));
        }
    }
}

void LogicalExtend::serialize(FileSerHelper& fsh) {
    fsh.writeString(typeid(LogicalExtend).name());
    fsh.writeString(boundNodeVarName);
    fsh.writeString(boundNodeVarLabel);
    fsh.writeString(nbrNodeVarName);
    fsh.writeString(nbrNodeVarLabel);
    fsh.writeString(relLabel);
    fsh.write(direction);
    LogicalOperator::serialize(fsh);
}

} // namespace processor
} // namespace graphflow
