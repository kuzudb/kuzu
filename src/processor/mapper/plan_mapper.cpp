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
    auto operatorType = logicalOperator->getLogicalOperatorType();
    switch (operatorType) {
    case LOGICAL_SCAN_NODE: {
        physicalOperator = mapLogicalScanNodeToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_INDEX_SCAN_NODE: {
        physicalOperator = mapLogicalIndexScanNodeToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_UNWIND: {
        physicalOperator = mapLogicalUnwindToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_EXTEND: {
        physicalOperator = mapLogicalExtendToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_FLATTEN: {
        physicalOperator = mapLogicalFlattenToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_FILTER: {
        physicalOperator = mapLogicalFilterToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_PROJECTION: {
        physicalOperator = mapLogicalProjectionToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_SEMI_MASKER: {
        physicalOperator = mapLogicalSemiMaskerToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_HASH_JOIN: {
        physicalOperator = mapLogicalHashJoinToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_INTERSECT: {
        physicalOperator = mapLogicalIntersectToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_CROSS_PRODUCT: {
        physicalOperator = mapLogicalCrossProductToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_SCAN_NODE_PROPERTY: {
        physicalOperator =
            mapLogicalScanNodePropertyToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_SCAN_REL_PROPERTY: {
        physicalOperator =
            mapLogicalScanRelPropertyToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_MULTIPLICITY_REDUCER: {
        physicalOperator =
            mapLogicalMultiplicityReducerToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_SKIP: {
        physicalOperator = mapLogicalSkipToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_LIMIT: {
        physicalOperator = mapLogicalLimitToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_AGGREGATE: {
        physicalOperator = mapLogicalAggregateToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_DISTINCT: {
        physicalOperator = mapLogicalDistinctToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_ORDER_BY: {
        physicalOperator = mapLogicalOrderByToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_UNION_ALL: {
        physicalOperator = mapLogicalUnionAllToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_ACCUMULATE: {
        physicalOperator = mapLogicalAccumulateToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_EXPRESSIONS_SCAN: {
        physicalOperator =
            mapLogicalExpressionsScanToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_FTABLE_SCAN: {
        physicalOperator = mapLogicalFTableScanToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_CREATE_NODE: {
        physicalOperator = mapLogicalCreateNodeToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_CREATE_REL: {
        physicalOperator = mapLogicalCreateRelToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_SET_NODE_PROPERTY: {
        physicalOperator = mapLogicalSetToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_DELETE_NODE: {
        physicalOperator = mapLogicalDeleteNodeToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_DELETE_REL: {
        physicalOperator = mapLogicalDeleteRelToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_CREATE_NODE_TABLE: {
        physicalOperator =
            mapLogicalCreateNodeTableToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_CREATE_REL_TABLE: {
        physicalOperator = mapLogicalCreateRelTableToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_COPY_CSV: {
        physicalOperator = mapLogicalCopyCSVToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_DROP_TABLE: {
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
        auto isFlat = schema.getGroup(expressionName)->getIsFlat();
        payloadsPosAndType.emplace_back(dataPos, expression->dataType);
        isPayloadFlat.push_back(isFlat);
    }
    auto sharedState = make_shared<FTableSharedState>();
    return make_unique<ResultCollector>(payloadsPosAndType, isPayloadFlat, sharedState,
        std::move(prevOperator), getOperatorID(), ExpressionUtil::toString(expressionsToCollect));
}

} // namespace processor
} // namespace kuzu
