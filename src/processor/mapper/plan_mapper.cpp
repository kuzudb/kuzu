#include "processor/mapper/plan_mapper.h"

#include <set>

#include "processor/mapper/expression_mapper.h"
#include "processor/operator/result_collector.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

unique_ptr<PhysicalPlan> PlanMapper::mapLogicalPlanToPhysical(LogicalPlan* logicalPlan) {
    auto mapperContext = MapperContext(make_unique<ResultSetDescriptor>(*logicalPlan->getSchema()));
    auto prevOperator = mapLogicalOperatorToPhysical(logicalPlan->getLastOperator(), mapperContext);
    auto lastOperator = appendResultCollector(logicalPlan->getExpressionsToCollect(),
        *logicalPlan->getSchema(), move(prevOperator), mapperContext);
    return make_unique<PhysicalPlan>(move(lastOperator), logicalPlan->isReadOnly());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalOperatorToPhysical(
    const shared_ptr<LogicalOperator>& logicalOperator, MapperContext& mapperContext) {
    unique_ptr<PhysicalOperator> physicalOperator;
    auto operatorType = logicalOperator->getOperatorType();
    switch (operatorType) {
    case LogicalOperatorType::SCAN_NODE: {
        physicalOperator = mapLogicalScanNodeToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::INDEX_SCAN_NODE: {
        physicalOperator = mapLogicalIndexScanNodeToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::UNWIND: {
        physicalOperator = mapLogicalUnwindToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::EXTEND: {
        physicalOperator = mapLogicalExtendToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::FLATTEN: {
        physicalOperator = mapLogicalFlattenToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::FILTER: {
        physicalOperator = mapLogicalFilterToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::PROJECTION: {
        physicalOperator = mapLogicalProjectionToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::SEMI_MASKER: {
        physicalOperator = mapLogicalSemiMaskerToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::HASH_JOIN: {
        physicalOperator = mapLogicalHashJoinToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::INTERSECT: {
        physicalOperator = mapLogicalIntersectToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::CROSS_PRODUCT: {
        physicalOperator = mapLogicalCrossProductToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::SCAN_NODE_PROPERTY: {
        physicalOperator =
            mapLogicalScanNodePropertyToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::MULTIPLICITY_REDUCER: {
        physicalOperator =
            mapLogicalMultiplicityReducerToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::SKIP: {
        physicalOperator = mapLogicalSkipToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::LIMIT: {
        physicalOperator = mapLogicalLimitToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::AGGREGATE: {
        physicalOperator = mapLogicalAggregateToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::DISTINCT: {
        physicalOperator = mapLogicalDistinctToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::ORDER_BY: {
        physicalOperator = mapLogicalOrderByToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::UNION_ALL: {
        physicalOperator = mapLogicalUnionAllToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::ACCUMULATE: {
        physicalOperator = mapLogicalAccumulateToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::EXPRESSIONS_SCAN: {
        physicalOperator =
            mapLogicalExpressionsScanToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::FTABLE_SCAN: {
        physicalOperator = mapLogicalFTableScanToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::CREATE_NODE: {
        physicalOperator = mapLogicalCreateNodeToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::CREATE_REL: {
        physicalOperator = mapLogicalCreateRelToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::SET_NODE_PROPERTY: {
        physicalOperator = mapLogicalSetToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::DELETE_NODE: {
        physicalOperator = mapLogicalDeleteNodeToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::DELETE_REL: {
        physicalOperator = mapLogicalDeleteRelToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::CREATE_NODE_TABLE: {
        physicalOperator =
            mapLogicalCreateNodeTableToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::CREATE_REL_TABLE: {
        physicalOperator = mapLogicalCreateRelTableToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::COPY_CSV: {
        physicalOperator = mapLogicalCopyCSVToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LogicalOperatorType::DROP_TABLE: {
        physicalOperator = mapLogicalDropTableToPhysical(logicalOperator.get(), mapperContext);
    } break;
    default:
        assert(false);
    }
    return physicalOperator;
}

unique_ptr<ResultCollector> PlanMapper::appendResultCollector(
    const expression_vector& expressionsToCollect, const Schema& schema,
    unique_ptr<PhysicalOperator> prevOperator, MapperContext& mapperContext) {
    vector<pair<DataPos, DataType>> payloadsPosAndType;
    vector<bool> isPayloadFlat;
    for (auto& expression : expressionsToCollect) {
        auto expressionName = expression->getUniqueName();
        auto dataPos = mapperContext.getDataPos(expressionName);
        auto isFlat = schema.getGroup(expressionName)->isFlat();
        payloadsPosAndType.emplace_back(dataPos, expression->dataType);
        isPayloadFlat.push_back(isFlat);
    }
    auto sharedState = make_shared<FTableSharedState>();
    return make_unique<ResultCollector>(mapperContext.getResultSetDescriptor()->copy(),
        payloadsPosAndType, isPayloadFlat, sharedState, std::move(prevOperator), getOperatorID(),
        ExpressionUtil::toString(expressionsToCollect));
}

} // namespace processor
} // namespace kuzu
