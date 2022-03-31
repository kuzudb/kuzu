#include "src/processor/include/physical_plan/operator/var_length_extend/var_length_extend.h"

namespace graphflow {
namespace processor {

VarLengthExtend::VarLengthExtend(const DataPos& boundNodeDataPos, const DataPos& nbrNodeDataPos,
    StorageStructure* storage, uint8_t lowerBound, uint8_t upperBound,
    unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
    : PhysicalOperator{move(child), context, id}, boundNodeDataPos{boundNodeDataPos},
      nbrNodeDataPos{nbrNodeDataPos}, storage{storage}, lowerBound{lowerBound}, upperBound{
                                                                                    upperBound} {
    dfsLevelInfos.resize(upperBound);
}

shared_ptr<ResultSet> VarLengthExtend::initResultSet() {
    resultSet = children[0]->initResultSet();
    boundNodeValueVector = resultSet->dataChunks[boundNodeDataPos.dataChunkPos]
                               ->valueVectors[boundNodeDataPos.valueVectorPos];
    nbrNodeValueVector = make_shared<ValueVector>(context.memoryManager, NODE);
    resultSet->dataChunks[nbrNodeDataPos.dataChunkPos]->insert(
        nbrNodeDataPos.valueVectorPos, nbrNodeValueVector);
    return resultSet;
}

void VarLengthExtend::reInitToRerunSubPlan() {
    while (!dfsStack.empty()) {
        dfsStack.pop();
    }
    children[0]->reInitToRerunSubPlan();
}

} // namespace processor
} // namespace graphflow
