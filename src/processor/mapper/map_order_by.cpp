#include "processor/mapper/plan_mapper.h"

#include "planner/logical_plan/logical_operator/logical_order_by.h"
#include "processor/operator/order_by/order_by.h"
#include "processor/operator/order_by/order_by_merge.h"
#include "processor/operator/order_by/order_by_scan.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalOrderByToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& logicalOrderBy = (LogicalOrderBy&)*logicalOperator;
    auto& schemaBeforeOrderBy = *logicalOrderBy.getSchemaBeforeOrderBy();
    auto mapperContextBeforeOrderBy =
        MapperContext(make_unique<ResultSetDescriptor>(schemaBeforeOrderBy));
    auto orderByPrevOperator =
        mapLogicalOperatorToPhysical(logicalOrderBy.getChild(0), mapperContextBeforeOrderBy);
    auto paramsString = logicalOrderBy.getExpressionsForPrinting();
    vector<DataPos> keyDataPoses;
    for (auto& expression : logicalOrderBy.getExpressionsToOrderBy()) {
        keyDataPoses.emplace_back(
            mapperContextBeforeOrderBy.getDataPos(expression->getUniqueName()));
    }
    vector<DataPos> inputDataPoses;
    vector<bool> isInputVectorFlat;
    vector<DataPos> outputDataPoses;
    for (auto& expression : logicalOrderBy.getExpressionsToMaterialize()) {
        auto expressionName = expression->getUniqueName();
        inputDataPoses.push_back(mapperContextBeforeOrderBy.getDataPos(expressionName));
        isInputVectorFlat.push_back(schemaBeforeOrderBy.getGroup(expressionName)->getIsFlat());
        outputDataPoses.push_back(mapperContext.getDataPos(expressionName));
        mapperContext.addComputedExpressions(expressionName);
    }

    auto orderByDataInfo = OrderByDataInfo(
        keyDataPoses, inputDataPoses, isInputVectorFlat, logicalOrderBy.getIsAscOrders());
    auto orderBySharedState = make_shared<SharedFactorizedTablesAndSortedKeyBlocks>();

    auto orderBy = make_unique<OrderBy>(orderByDataInfo, orderBySharedState,
        move(orderByPrevOperator), getOperatorID(), paramsString);
    auto orderByMerge =
        make_unique<OrderByMerge>(orderBySharedState, move(orderBy), getOperatorID(), paramsString);
    auto orderByScan = make_unique<OrderByScan>(mapperContext.getResultSetDescriptor()->copy(),
        outputDataPoses, orderBySharedState, move(orderByMerge), getOperatorID(), paramsString);
    return orderByScan;
}

} // namespace processor
} // namespace kuzu
