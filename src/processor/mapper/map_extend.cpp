#include "planner/logical_plan/logical_operator/logical_extend.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/generic_extend.h"
#include "processor/operator/scan_column/adj_column_extend.h"
#include "processor/operator/scan_list/adj_list_extend.h"
#include "processor/operator/var_length_extend/var_length_adj_list_extend.h"
#include "processor/operator/var_length_extend/var_length_column_extend.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalExtendToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto extend = (LogicalExtend*)logicalOperator;
    auto boundNode = extend->getBoundNode();
    auto nbrNode = extend->getNbrNode();
    auto rel = extend->getRel();
    auto direction = extend->getDirection();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto inDataPos = mapperContext.getDataPos(boundNode->getInternalIDPropertyName());
    auto outDataPos = mapperContext.getDataPos(nbrNode->getInternalIDPropertyName());
    mapperContext.addComputedExpressions(nbrNode->getInternalIDPropertyName());
    auto& relsStore = storageManager.getRelsStore();
    auto paramsString = extend->getExpressionsForPrinting();
    auto boundNodeTableID = boundNode->getTableID();
    if (rel->getNumTableIDs() > 1) { // map to generic extend with single bound node
        vector<Column*> columns;
        vector<Lists*> lists;
        for (auto relTableID : rel->getTableIDs()) {
            if (relsStore.hasAdjColumn(direction, boundNodeTableID, relTableID)) {
                columns.push_back(relsStore.getAdjColumn(direction, boundNodeTableID, relTableID));
            }
            if (relsStore.hasAdjList(direction, boundNodeTableID, relTableID)) {
                lists.push_back(relsStore.getAdjLists(direction, boundNodeTableID, relTableID));
            }
        }
        return make_unique<GenericExtend>(inDataPos, outDataPos, std::move(columns),
            std::move(lists), std::move(prevOperator), getOperatorID(), paramsString);
    } else { // map to adjList/Column with single bound node
        auto relTableID = *rel->getTableIDs().begin();
        if (relsStore.hasAdjColumn(direction, boundNodeTableID, relTableID)) {
            auto column = relsStore.getAdjColumn(direction, boundNodeTableID, relTableID);
            if (rel->isVariableLength()) {
                return make_unique<VarLengthColumnExtend>(inDataPos, outDataPos, column,
                    rel->getLowerBound(), rel->getUpperBound(), std::move(prevOperator),
                    getOperatorID(), paramsString);
            } else {
                return make_unique<AdjColumnExtend>(inDataPos, outDataPos, column,
                    std::move(prevOperator), getOperatorID(), paramsString);
            }
        } else {
            assert(relsStore.hasAdjList(direction, boundNodeTableID, relTableID));
            auto list = relsStore.getAdjLists(direction, boundNodeTableID, relTableID);
            if (rel->isVariableLength()) {
                return make_unique<VarLengthAdjListExtend>(inDataPos, outDataPos, list,
                    rel->getLowerBound(), rel->getUpperBound(), std::move(prevOperator),
                    getOperatorID(), paramsString);
            } else {
                return make_unique<AdjListExtend>(inDataPos, outDataPos, list,
                    std::move(prevOperator), getOperatorID(), paramsString);
            }
        }
    }
}

} // namespace processor
} // namespace kuzu
