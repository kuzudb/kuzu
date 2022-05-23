#include "src/processor/include/physical_plan/operator/var_length_extend/var_length_extend.h"

namespace graphflow {
namespace processor {

VarLengthExtend::VarLengthExtend(const DataPos& boundNodeDataPos, const DataPos& nbrNodeDataPos,
    BaseColumnOrList* storage, uint8_t lowerBound, uint8_t upperBound,
    unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
    : PhysicalOperator{move(child), id, paramsString}, boundNodeDataPos{boundNodeDataPos},
      nbrNodeDataPos{nbrNodeDataPos}, storage{storage}, lowerBound{lowerBound}, upperBound{
                                                                                    upperBound} {
    dfsLevelInfos.resize(upperBound);
}

shared_ptr<ResultSet> VarLengthExtend::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    boundNodeValueVector = resultSet->dataChunks[boundNodeDataPos.dataChunkPos]
                               ->valueVectors[boundNodeDataPos.valueVectorPos];
    nbrNodeValueVector = make_shared<ValueVector>(context->memoryManager, NODE);
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
