#include "processor/mapper/plan_mapper.h"

#include <set>

#include "planner/logical_plan/logical_operator/logical_shortest_path.h"
#include "processor/mapper/expression_mapper.h"
#include "processor/operator/result_collector.h"
#include "processor/operator/shortest_path_adj_col.h"
#include "processor/operator/shortest_path_adj_list.h"

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
    case LOGICAL_SHORTEST_PATH: {
        physicalOperator = mapLogicalShortestPathToPhysical(logicalOperator.get(), mapperContext);
    } break;
    default:
        assert(false);
    }
    return physicalOperator;
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalShortestPathToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto* logicalShortestPath = (LogicalShortestPath*)logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto sourceNode = logicalShortestPath->getSrcNodeExpression();
    auto destNode = logicalShortestPath->getDestNodeExpression();
    auto rel = logicalShortestPath->getRelExpression();
    // assume this here for now
    auto direction = REL_DIRECTIONS[0];
    auto srcDataPos = mapperContext.getDataPos(sourceNode->getInternalIDPropertyName());
    auto destDataPos = mapperContext.getDataPos(destNode->getInternalIDPropertyName());
    auto& relStore = storageManager.getRelsStore();
    if (relStore.hasAdjColumn(direction, sourceNode->getTableID(), rel->getTableID())) {
        Column* column =
            relStore.getAdjColumn(direction, sourceNode->getTableID(), rel->getTableID());
        return make_unique<ShortestPathAdjCol>(srcDataPos, destDataPos, column,
            rel->getLowerBound(), rel->getUpperBound(), move(prevOperator), getOperatorID(),
            logicalShortestPath->getExpressionsForPrinting());
    } else {
        assert(relStore.hasAdjList(direction, sourceNode->getTableID(), rel->getTableID()));
        auto relProperties =
            (PropertyExpression*)logicalShortestPath->getRelPropertyExpression().get();
        auto relIDLists = relStore.getRelPropertyLists(direction, sourceNode->getTableID(),
            rel->getTableID(), relProperties->getPropertyID(rel->getTableID()));
        AdjLists* adjLists =
            relStore.getAdjLists(direction, sourceNode->getTableID(), rel->getTableID());
        return make_unique<ShortestPathAdjList>(srcDataPos, destDataPos, adjLists, relIDLists,
            rel->getLowerBound(), rel->getUpperBound(), move(prevOperator), getOperatorID(),
            logicalShortestPath->getExpressionsForPrinting());
    }
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
    return make_unique<ResultCollector>(payloadsPosAndType, isPayloadFlat, sharedState,
        std::move(prevOperator), getOperatorID(), ExpressionUtil::toString(expressionsToCollect));
}

} // namespace processor
} // namespace kuzu
