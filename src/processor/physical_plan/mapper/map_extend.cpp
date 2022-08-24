#include "src/planner/logical_plan/logical_operator/include/logical_extend.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"
#include "src/processor/include/physical_plan/operator/read_list/adj_list_extend.h"
#include "src/processor/include/physical_plan/operator/scan_column/adj_column_extend.h"
#include "src/processor/include/physical_plan/operator/var_length_extend/var_length_adj_list_extend.h"
#include "src/processor/include/physical_plan/operator/var_length_extend/var_length_column_extend.h"

namespace graphflow {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalExtendToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto extend = (LogicalExtend*)logicalOperator;
    auto boundNode = extend->getBoundNodeExpression();
    auto nbrNode = extend->getNbrNodeExpression();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto inDataPos = mapperContext.getDataPos(boundNode->getIDProperty());
    auto outDataPos = mapperContext.getDataPos(nbrNode->getIDProperty());
    mapperContext.addComputedExpressions(nbrNode->getIDProperty());
    auto& relsStore = storageManager.getRelsStore();
    auto lowerBound = extend->getLowerBound();
    auto upperBound = extend->getUpperBound();
    auto paramsString = extend->getExpressionsForPrinting();
    if (extend->getIsColumn()) {
        auto adjColumn = relsStore.getAdjColumn(
            extend->getDirection(), boundNode->getTableID(), extend->getRelTableID());
        if (lowerBound == 1 && lowerBound == upperBound) {
            return make_unique<AdjColumnExtend>(inDataPos, outDataPos, adjColumn,
                move(prevOperator), getOperatorID(), paramsString);
        } else {
            return make_unique<VarLengthColumnExtend>(inDataPos, outDataPos, adjColumn, lowerBound,
                upperBound, move(prevOperator), getOperatorID(), paramsString);
        }
    } else {
        auto adjLists = relsStore.getAdjLists(
            extend->getDirection(), boundNode->getTableID(), extend->getRelTableID());
        if (lowerBound == 1 && lowerBound == upperBound) {
            return make_unique<AdjListExtend>(
                inDataPos, outDataPos, adjLists, move(prevOperator), getOperatorID(), paramsString);
        } else {
            return make_unique<VarLengthAdjListExtend>(inDataPos, outDataPos, adjLists, lowerBound,
                upperBound, move(prevOperator), getOperatorID(), paramsString);
        }
    }
}

} // namespace processor
} // namespace graphflow
