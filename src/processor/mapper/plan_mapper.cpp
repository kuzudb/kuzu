#include "processor/mapper/plan_mapper.h"

#include <set>

#include "processor/mapper/expression_mapper.h"
#include "processor/operator/result_collector.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

unique_ptr<PhysicalPlan> PlanMapper::mapLogicalPlanToPhysical(LogicalPlan* logicalPlan,
    const expression_vector& expressionsToCollect, common::StatementType statementType) {
    auto lastOperator = mapLogicalOperatorToPhysical(logicalPlan->getLastOperator());
    if (!StatementTypeUtils::isCopyCSV(statementType)) {
        lastOperator = appendResultCollector(
            expressionsToCollect, *logicalPlan->getSchema(), std::move(lastOperator));
    }
    return make_unique<PhysicalPlan>(std::move(lastOperator));
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalOperatorToPhysical(
    const shared_ptr<LogicalOperator>& logicalOperator) {
    unique_ptr<PhysicalOperator> physicalOperator;
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
    case LogicalOperatorType::DROP_PROPERTY: {
        physicalOperator = mapLogicalDropPropertyToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::DROP_TABLE: {
        physicalOperator = mapLogicalDropTableToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::ADD_PROPERTY: {
        physicalOperator = mapLogicalAddPropertyToPhysical(logicalOperator.get());
    } break;
    default:
        assert(false);
    }
    return physicalOperator;
}

unique_ptr<ResultCollector> PlanMapper::appendResultCollector(
    const expression_vector& expressionsToCollect, const Schema& schema,
    unique_ptr<PhysicalOperator> prevOperator) {
    vector<pair<DataPos, DataType>> payloadsPosAndType;
    vector<bool> isPayloadFlat;
    for (auto& expression : expressionsToCollect) {
        auto expressionName = expression->getUniqueName();
        auto dataPos = DataPos(schema.getExpressionPos(*expression));
        auto isFlat = schema.getGroup(expressionName)->isFlat();
        payloadsPosAndType.emplace_back(dataPos, expression->dataType);
        isPayloadFlat.push_back(isFlat);
    }
    auto sharedState = make_shared<FTableSharedState>();
    return make_unique<ResultCollector>(make_unique<ResultSetDescriptor>(schema),
        payloadsPosAndType, isPayloadFlat, sharedState, std::move(prevOperator), getOperatorID(),
        ExpressionUtil::toString(expressionsToCollect));
}

} // namespace processor
} // namespace kuzu
