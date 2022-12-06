#include "planner/logical_plan/logical_operator/logical_order_by.h"
#include "processor/mapper/plan_mapper.h"
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
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOrderBy.getChild(0), mapperContextBeforeOrderBy);
    auto paramsString = logicalOrderBy.getExpressionsForPrinting();
    vector<pair<DataPos, DataType>> keysPosAndType;
    for (auto& expression : logicalOrderBy.getExpressionsToOrderBy()) {
        keysPosAndType.emplace_back(
            mapperContextBeforeOrderBy.getDataPos(expression->getUniqueName()),
            expression->dataType);
    }
    vector<pair<DataPos, DataType>> payloadsPosAndType;
    vector<bool> isPayloadFlat;
    vector<DataPos> outVectorPos;
    for (auto& expression : logicalOrderBy.getExpressionsToMaterialize()) {
        auto expressionName = expression->getUniqueName();
        payloadsPosAndType.emplace_back(
            mapperContextBeforeOrderBy.getDataPos(expressionName), expression->dataType);
        isPayloadFlat.push_back(schemaBeforeOrderBy.getGroup(expressionName)->isFlat());
        outVectorPos.emplace_back(mapperContext.getDataPos(expressionName));
        mapperContext.addComputedExpressions(expressionName);
    }
    // See comment in planOrderBy in projectionPlanner.cpp
    auto mayContainUnflatKey = logicalOrderBy.getSchemaBeforeOrderBy()->getNumGroups() == 1;
    auto orderByDataInfo = OrderByDataInfo(keysPosAndType, payloadsPosAndType, isPayloadFlat,
        logicalOrderBy.getIsAscOrders(), mayContainUnflatKey);
    auto orderBySharedState = make_shared<SharedFactorizedTablesAndSortedKeyBlocks>();

    auto orderBy = make_unique<OrderBy>(mapperContextBeforeOrderBy.getResultSetDescriptor()->copy(),
        orderByDataInfo, orderBySharedState, std::move(prevOperator), getOperatorID(),
        paramsString);
    auto dispatcher = make_shared<KeyBlockMergeTaskDispatcher>();
    auto orderByMerge = make_unique<OrderByMerge>(orderBySharedState, std::move(dispatcher),
        std::move(orderBy), getOperatorID(), paramsString);
    auto orderByScan = make_unique<OrderByScan>(
        outVectorPos, orderBySharedState, std::move(orderByMerge), getOperatorID(), paramsString);
    return orderByScan;
}

} // namespace processor
} // namespace kuzu
