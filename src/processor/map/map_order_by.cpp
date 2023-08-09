#include "planner/logical_plan/logical_order_by.h"
#include "processor/operator/order_by/order_by.h"
#include "processor/operator/order_by/order_by_merge.h"
#include "processor/operator/order_by/order_by_scan.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapOrderBy(LogicalOperator* logicalOperator) {
    auto& logicalOrderBy = (LogicalOrderBy&)*logicalOperator;
    auto outSchema = logicalOrderBy.getSchema();
    auto inSchema = logicalOrderBy.getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOrderBy.getChild(0).get());
    auto paramsString = logicalOrderBy.getExpressionsForPrinting();
    std::vector<std::pair<DataPos, LogicalType>> keysPosAndType;
    for (auto& expression : logicalOrderBy.getExpressionsToOrderBy()) {
        keysPosAndType.emplace_back(inSchema->getExpressionPos(*expression), expression->dataType);
    }
    std::vector<std::pair<DataPos, LogicalType>> payloadsPosAndType;
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
