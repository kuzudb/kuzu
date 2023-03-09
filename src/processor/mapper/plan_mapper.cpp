#include "processor/mapper/plan_mapper.h"

#include <set>

#include "processor/mapper/expression_mapper.h"
#include "processor/operator/result_collector.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalPlan> PlanMapper::mapLogicalPlanToPhysical(LogicalPlan* logicalPlan,
    const binder::expression_vector& expressionsToCollect, common::StatementType statementType) {
    auto lastOperator = mapLogicalOperatorToPhysical(logicalPlan->getLastOperator());
    if (!StatementTypeUtils::isCopyCSV(statementType)) {
        lastOperator = appendResultCollector(
            expressionsToCollect, *logicalPlan->getSchema(), std::move(lastOperator));
    }
    return make_unique<PhysicalPlan>(std::move(lastOperator));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalOperatorToPhysical(
    const std::shared_ptr<LogicalOperator>& logicalOperator) {
    std::unique_ptr<PhysicalOperator> physicalOperator;
    auto operatorType = logicalOperator->getOperatorType();
    switch (operatorType) {
    case LogicalOperatorType::SCAN_NODE: {
        physicalOperator = mapLogicalScanNodeToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::INDEX_SCAN_NODE: {
        physicalOperator = mapLogicalIndexScanNodeToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::UNWIND: {
        physicalOperator = mapLogicalUnwindToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::EXTEND: {
        physicalOperator = mapLogicalExtendToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::FLATTEN: {
        physicalOperator = mapLogicalFlattenToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::FILTER: {
        physicalOperator = mapLogicalFilterToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::PROJECTION: {
        physicalOperator = mapLogicalProjectionToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::SEMI_MASKER: {
        physicalOperator = mapLogicalSemiMaskerToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::HASH_JOIN: {
        physicalOperator = mapLogicalHashJoinToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::INTERSECT: {
        physicalOperator = mapLogicalIntersectToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::CROSS_PRODUCT: {
        physicalOperator = mapLogicalCrossProductToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::SCAN_NODE_PROPERTY: {
        physicalOperator = mapLogicalScanNodePropertyToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::MULTIPLICITY_REDUCER: {
        physicalOperator = mapLogicalMultiplicityReducerToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::SKIP: {
        physicalOperator = mapLogicalSkipToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::LIMIT: {
        physicalOperator = mapLogicalLimitToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::AGGREGATE: {
        physicalOperator = mapLogicalAggregateToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::DISTINCT: {
        physicalOperator = mapLogicalDistinctToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::ORDER_BY: {
        physicalOperator = mapLogicalOrderByToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::UNION_ALL: {
        physicalOperator = mapLogicalUnionAllToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::ACCUMULATE: {
        physicalOperator = mapLogicalAccumulateToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::EXPRESSIONS_SCAN: {
        physicalOperator = mapLogicalExpressionsScanToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::FTABLE_SCAN: {
        physicalOperator = mapLogicalFTableScanToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::CREATE_NODE: {
        physicalOperator = mapLogicalCreateNodeToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::CREATE_REL: {
        physicalOperator = mapLogicalCreateRelToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::SET_NODE_PROPERTY: {
        physicalOperator = mapLogicalSetNodePropertyToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::SET_REL_PROPERTY: {
        physicalOperator = mapLogicalSetRelPropertyToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::DELETE_NODE: {
        physicalOperator = mapLogicalDeleteNodeToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::DELETE_REL: {
        physicalOperator = mapLogicalDeleteRelToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::CREATE_NODE_TABLE: {
        physicalOperator = mapLogicalCreateNodeTableToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::CREATE_REL_TABLE: {
        physicalOperator = mapLogicalCreateRelTableToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::COPY_CSV: {
        physicalOperator = mapLogicalCopyToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::DROP_TABLE: {
        physicalOperator = mapLogicalDropTableToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::RENAME_TABLE: {
        physicalOperator = mapLogicalRenameTableToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::ADD_PROPERTY: {
        physicalOperator = mapLogicalAddPropertyToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::DROP_PROPERTY: {
        physicalOperator = mapLogicalDropPropertyToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::RENAME_PROPERTY: {
        physicalOperator = mapLogicalRenamePropertyToPhysical(logicalOperator.get());
    } break;
    default:
        throw common::NotImplementedException("PlanMapper::mapLogicalOperatorToPhysical()");
    }
    logicalOpToPhysicalOpMap.insert({logicalOperator.get(), physicalOperator.get()});
    return physicalOperator;
}

std::unique_ptr<ResultCollector> PlanMapper::appendResultCollector(
    const binder::expression_vector& expressionsToCollect, const Schema& schema,
    std::unique_ptr<PhysicalOperator> prevOperator) {
    std::vector<std::pair<DataPos, DataType>> payloadsPosAndType;
    std::vector<bool> isPayloadFlat;
    for (auto& expression : expressionsToCollect) {
        auto expressionName = expression->getUniqueName();
        auto dataPos = DataPos(schema.getExpressionPos(*expression));
        auto isFlat = schema.getGroup(expressionName)->isFlat();
        payloadsPosAndType.emplace_back(dataPos, expression->dataType);
        isPayloadFlat.push_back(isFlat);
    }
    auto sharedState = std::make_shared<FTableSharedState>();
    return make_unique<ResultCollector>(std::make_unique<ResultSetDescriptor>(schema),
        payloadsPosAndType, isPayloadFlat, sharedState, std::move(prevOperator), getOperatorID(),
        binder::ExpressionUtil::toString(expressionsToCollect));
}

} // namespace processor
} // namespace kuzu
