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
    const Graph& graph, VarToChunkAndVectorIdxMap& schema) {
    auto prevOperator = this->prevOperator->mapToPhysical(graph, schema);
    auto inDataChunkIdx = schema.getDataChunkPos(boundNodeVarName);
    auto inValueVectorIdx = schema.getValueVectorPos(boundNodeVarName);
    auto catalog = graph.getCatalog();
    label_t relLabelFromString = catalog.getRelLabelFromString(relLabel);
    auto dataChunks = prevOperator->getOutDataChunks();
    auto numChunks = dataChunks->getNumDataChunks();
    auto nodeLabel = catalog.getNodeLabelFromString(boundNodeVarLabel);
    if (catalog.isSingleCaridinalityInDir(relLabelFromString, direction)) {
        auto dataChunkPos = numChunks - 1;
        auto valueVectorPos = dataChunks->getNumValueVectors(dataChunkPos);
        schema.put(nbrNodeVarName, dataChunkPos, valueVectorPos);
        return make_unique<AdjColumnExtend>(inDataChunkIdx, inValueVectorIdx,
            graph.getAdjColumn(direction, nodeLabel, relLabelFromString), move(prevOperator));
    } else {
        schema.put(nbrNodeVarName, numChunks /*dataChunkPos*/, 0 /*valueVectorPos*/);
        if (dataChunks->getDataChunk(inDataChunkIdx)->isFlat) {
            return make_unique<AdjListOnlyExtend>(inDataChunkIdx, inValueVectorIdx,
                graph.getAdjLists(direction, nodeLabel, relLabelFromString), move(prevOperator));
        } else {
            return make_unique<AdjListFlattenAndExtend>(inDataChunkIdx, inValueVectorIdx,
                graph.getAdjLists(direction, nodeLabel, relLabelFromString), move(prevOperator));
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
