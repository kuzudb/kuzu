#include "planner/logical_plan/logical_operator/logical_order_by.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/order_by/order_by.h"
#include "processor/operator/order_by/order_by_merge.h"
#include "processor/operator/order_by/order_by_scan.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalOrderByToPhysical(
    LogicalOperator* logicalOperator) {
    auto& logicalOrderBy = (LogicalOrderBy&)*logicalOperator;
    auto outSchema = logicalOrderBy.getSchema();
    auto inSchema = logicalOrderBy.getChild(0)->getSchema();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOrderBy.getChild(0));
    auto paramsString = logicalOrderBy.getExpressionsForPrinting();
    std::vector<std::pair<DataPos, common::LogicalType>> keysPosAndType;
    for (auto& expression : logicalOrderBy.getExpressionsToOrderBy()) {
        keysPosAndType.emplace_back(inSchema->getExpressionPos(*expression), expression->dataType);
    }
    std::vector<std::pair<DataPos, common::LogicalType>> payloadsPosAndType;
    std::vector<bool> isPayloadFlat;
    std::vector<DataPos> outVectorPos;
    for (auto& expression : logicalOrderBy.getExpressionsToMaterialize()) {
        auto expressionName = expression->getUniqueName();
        payloadsPosAndType.emplace_back(
            inSchema->getExpressionPos(*expression), expression->dataType);
        isPayloadFlat.push_back(inSchema->getGroup(expressionName)->isFlat());
        outVectorPos.emplace_back(outSchema->getExpressionPos(*expression));
    }
    // See comment in planOrderBy in projectionPlanner.cpp
    auto mayContainUnflatKey = inSchema->getNumGroups() == 1;
    auto orderByDataInfo = OrderByDataInfo(keysPosAndType, payloadsPosAndType, isPayloadFlat,
        logicalOrderBy.getIsAscOrders(), mayContainUnflatKey);
    auto orderBySharedState = std::make_shared<SharedFactorizedTablesAndSortedKeyBlocks>();

    auto orderBy =
        make_unique<OrderBy>(std::make_unique<ResultSetDescriptor>(inSchema), orderByDataInfo,
            orderBySharedState, std::move(prevOperator), getOperatorID(), paramsString);
    auto dispatcher = std::make_shared<KeyBlockMergeTaskDispatcher>();
    auto orderByMerge = make_unique<OrderByMerge>(orderBySharedState, std::move(dispatcher),
        std::move(orderBy), getOperatorID(), paramsString);
    auto orderByScan = make_unique<OrderByScan>(
        outVectorPos, orderBySharedState, std::move(orderByMerge), getOperatorID(), paramsString);
    return orderByScan;
}

} // namespace processor
} // namespace kuzu
