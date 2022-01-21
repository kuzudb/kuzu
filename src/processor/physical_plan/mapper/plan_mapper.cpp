#include "src/processor/include/physical_plan/mapper/plan_mapper.h"

#include <set>

#include "src/function/include/aggregate/aggregate_function.h"
#include "src/planner/include/logical_plan/operator/aggregate/logical_aggregate.h"
#include "src/planner/include/logical_plan/operator/exists/logical_exist.h"
#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/filter/logical_filter.h"
#include "src/planner/include/logical_plan/operator/flatten/logical_flatten.h"
#include "src/planner/include/logical_plan/operator/hash_join/logical_hash_join.h"
#include "src/planner/include/logical_plan/operator/intersect/logical_intersect.h"
#include "src/planner/include/logical_plan/operator/limit/logical_limit.h"
#include "src/planner/include/logical_plan/operator/nested_loop_join/logical_left_nested_loop_join.h"
#include "src/planner/include/logical_plan/operator/order_by/logical_order_by.h"
#include "src/planner/include/logical_plan/operator/projection/logical_projection.h"
#include "src/planner/include/logical_plan/operator/scan_node_id/logical_scan_node_id.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_node_property.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_rel_property.h"
#include "src/planner/include/logical_plan/operator/select_scan/logical_result_scan.h"
#include "src/planner/include/logical_plan/operator/skip/logical_skip.h"
#include "src/processor/include/physical_plan/mapper/expression_mapper.h"
#include "src/processor/include/physical_plan/operator/aggregate/simple_aggregate.h"
#include "src/processor/include/physical_plan/operator/aggregate/simple_aggregate_scan.h"
#include "src/processor/include/physical_plan/operator/exists.h"
#include "src/processor/include/physical_plan/operator/filter.h"
#include "src/processor/include/physical_plan/operator/flatten.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"
#include "src/processor/include/physical_plan/operator/intersect.h"
#include "src/processor/include/physical_plan/operator/left_nested_loop_join.h"
#include "src/processor/include/physical_plan/operator/limit.h"
#include "src/processor/include/physical_plan/operator/multiplicity_reducer.h"
#include "src/processor/include/physical_plan/operator/order_by/order_by.h"
#include "src/processor/include/physical_plan/operator/order_by/order_by_merge.h"
#include "src/processor/include/physical_plan/operator/order_by/order_by_scan.h"
#include "src/processor/include/physical_plan/operator/projection.h"
#include "src/processor/include/physical_plan/operator/read_list/adj_list_extend.h"
#include "src/processor/include/physical_plan/operator/read_list/frontier_extend.h"
#include "src/processor/include/physical_plan/operator/read_list/read_rel_property_list.h"
#include "src/processor/include/physical_plan/operator/result_collector.h"
#include "src/processor/include/physical_plan/operator/result_scan.h"
#include "src/processor/include/physical_plan/operator/scan_attribute/adj_column_extend.h"
#include "src/processor/include/physical_plan/operator/scan_attribute/scan_structured_property.h"
#include "src/processor/include/physical_plan/operator/scan_attribute/scan_unstructured_property.h"
#include "src/processor/include/physical_plan/operator/scan_node_id.h"
#include "src/processor/include/physical_plan/operator/skip.h"

using namespace graphflow::planner;

namespace graphflow {
namespace processor {

unique_ptr<PhysicalPlan> PlanMapper::mapLogicalPlanToPhysical(
    unique_ptr<LogicalPlan> logicalPlan, ExecutionContext& executionContext) {
    auto mapperContext = MapperContext(make_unique<ResultSetDescriptor>(*logicalPlan->schema));
    vector<DataPos> valueVectorsToCollectPos;
    for (auto& expression : logicalPlan->getExpressionsToCollect()) {
        valueVectorsToCollectPos.push_back(mapperContext.getDataPos(expression->getUniqueName()));
    }
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalPlan->lastOperator, mapperContext, executionContext);
    auto resultCollector = make_unique<ResultCollector>(move(valueVectorsToCollectPos),
        move(prevOperator), executionContext, mapperContext.getOperatorID());
    return make_unique<PhysicalPlan>(move(resultCollector));
}

const MapperContext* PlanMapper::enterSubquery(const MapperContext* newMapperContext) {
    auto prevMapperContext = outerMapperContext;
    outerMapperContext = newMapperContext;
    return prevMapperContext;
}

void PlanMapper::exitSubquery(const MapperContext* prevMapperContext) {
    outerMapperContext = prevMapperContext;
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalOperatorToPhysical(
    const shared_ptr<LogicalOperator>& logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    unique_ptr<PhysicalOperator> physicalOperator;
    auto operatorType = logicalOperator->getLogicalOperatorType();
    switch (operatorType) {
    case LOGICAL_SCAN_NODE_ID: {
        physicalOperator =
            mapLogicalScanNodeIDToPhysical(logicalOperator.get(), mapperContext, executionContext);
    } break;
    case LOGICAL_SELECT_SCAN: {
        physicalOperator =
            mapLogicalResultScanToPhysical(logicalOperator.get(), mapperContext, executionContext);
    } break;
    case LOGICAL_EXTEND: {
        physicalOperator =
            mapLogicalExtendToPhysical(logicalOperator.get(), mapperContext, executionContext);
    } break;
    case LOGICAL_FLATTEN: {
        physicalOperator =
            mapLogicalFlattenToPhysical(logicalOperator.get(), mapperContext, executionContext);
    } break;
    case LOGICAL_FILTER: {
        physicalOperator =
            mapLogicalFilterToPhysical(logicalOperator.get(), mapperContext, executionContext);
    } break;
    case LOGICAL_INTERSECT: {
        physicalOperator =
            mapLogicalIntersectToPhysical(logicalOperator.get(), mapperContext, executionContext);
    } break;
    case LOGICAL_PROJECTION: {
        physicalOperator =
            mapLogicalProjectionToPhysical(logicalOperator.get(), mapperContext, executionContext);
    } break;
    case LOGICAL_HASH_JOIN: {
        physicalOperator =
            mapLogicalHashJoinToPhysical(logicalOperator.get(), mapperContext, executionContext);
    } break;
    case LOGICAL_SCAN_NODE_PROPERTY: {
        physicalOperator = mapLogicalScanNodePropertyToPhysical(
            logicalOperator.get(), mapperContext, executionContext);
    } break;
    case LOGICAL_SCAN_REL_PROPERTY: {
        physicalOperator = mapLogicalScanRelPropertyToPhysical(
            logicalOperator.get(), mapperContext, executionContext);
    } break;
    case LOGICAL_MULTIPLICITY_REDUCER: {
        physicalOperator = mapLogicalMultiplicityReducerToPhysical(
            logicalOperator.get(), mapperContext, executionContext);
    } break;
    case LOGICAL_SKIP: {
        physicalOperator =
            mapLogicalSkipToPhysical(logicalOperator.get(), mapperContext, executionContext);
    } break;
    case LOGICAL_LIMIT: {
        physicalOperator =
            mapLogicalLimitToPhysical(logicalOperator.get(), mapperContext, executionContext);
    } break;
    case LOGICAL_AGGREGATE: {
        physicalOperator =
            mapLogicalAggregateToPhysical(logicalOperator.get(), mapperContext, executionContext);
    } break;
    case LOGICAL_EXISTS: {
        physicalOperator =
            mapLogicalExistsToPhysical(logicalOperator.get(), mapperContext, executionContext);
    } break;
    case LOGICAL_LEFT_NESTED_LOOP_JOIN: {
        physicalOperator = mapLogicalLeftNestedLoopJoinToPhysical(
            logicalOperator.get(), mapperContext, executionContext);
    } break;
    case LOGICAL_ORDER_BY: {
        physicalOperator =
            mapLogicalOrderByToPhysical(logicalOperator.get(), mapperContext, executionContext);
    } break;
    default:
        assert(false);
    }
    // This map is populated regardless of whether PROFILE or EXPLAIN is enabled.
    physicalIDToLogicalOperatorMap.insert({physicalOperator->getOperatorID(), logicalOperator});
    return physicalOperator;
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanNodeIDToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& logicalScan = (const LogicalScanNodeID&)*logicalOperator;
    auto morsel = make_shared<MorselsDesc>(graph.getNumNodes(logicalScan.label));
    auto dataPos = mapperContext.getDataPos(logicalScan.nodeID);
    mapperContext.addComputedExpressions(logicalScan.nodeID);
    return make_unique<ScanNodeID>(mapperContext.getResultSetDescriptor()->copy(),
        logicalScan.label, dataPos, morsel, executionContext, mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalResultScanToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& resultScan = (const LogicalResultScan&)*logicalOperator;
    vector<DataPos> inDataPoses;
    uint32_t outDataChunkPos =
        mapperContext.getDataPos(*resultScan.getVariablesToSelect().begin()).dataChunkPos;
    vector<uint32_t> outValueVectorsPos;
    for (auto& variable : resultScan.getVariablesToSelect()) {
        inDataPoses.push_back(outerMapperContext->getDataPos(variable));
        // all variables should be appended to the same datachunk
        assert(outDataChunkPos == mapperContext.getDataPos(variable).dataChunkPos);
        outValueVectorsPos.push_back(mapperContext.getDataPos(variable).valueVectorPos);
        mapperContext.addComputedExpressions(variable);
    }
    return make_unique<ResultScan>(mapperContext.getResultSetDescriptor()->copy(),
        move(inDataPoses), outDataChunkPos, move(outValueVectorsPos), executionContext,
        mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalExtendToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& extend = (const LogicalExtend&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(
        logicalOperator->getFirstChild(), mapperContext, executionContext);
    auto inDataPos = mapperContext.getDataPos(extend.boundNodeID);
    auto outDataPos = mapperContext.getDataPos(extend.nbrNodeID);
    mapperContext.addComputedExpressions(extend.nbrNodeID);
    auto& relsStore = graph.getRelsStore();
    if (extend.isColumn) {
        assert(extend.lowerBound == extend.upperBound && extend.lowerBound == 1);
        return make_unique<AdjColumnExtend>(inDataPos, outDataPos,
            relsStore.getAdjColumn(extend.direction, extend.boundNodeLabel, extend.relLabel),
            move(prevOperator), executionContext, mapperContext.getOperatorID());
    } else {
        auto adjLists =
            relsStore.getAdjLists(extend.direction, extend.boundNodeLabel, extend.relLabel);
        if (extend.lowerBound == 1 && extend.lowerBound == extend.upperBound) {
            return make_unique<AdjListExtend>(inDataPos, outDataPos, adjLists, move(prevOperator),
                executionContext, mapperContext.getOperatorID());
        } else {
            return make_unique<FrontierExtend>(inDataPos, outDataPos, adjLists, extend.nbrNodeLabel,
                extend.lowerBound, extend.upperBound, move(prevOperator), executionContext,
                mapperContext.getOperatorID());
        }
    }
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalFlattenToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& flatten = (const LogicalFlatten&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(
        logicalOperator->getFirstChild(), mapperContext, executionContext);
    auto dataChunkPos = mapperContext.getDataPos(flatten.variable).dataChunkPos;
    return make_unique<Flatten>(
        dataChunkPos, move(prevOperator), executionContext, mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalFilterToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& logicalFilter = (const LogicalFilter&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(
        logicalOperator->getFirstChild(), mapperContext, executionContext);
    auto dataChunkToSelectPos = logicalFilter.groupPosToSelect;
    auto physicalRootExpr = expressionMapper.mapLogicalExpressionToPhysical(
        *logicalFilter.expression, mapperContext, executionContext);
    return make_unique<Filter>(move(physicalRootExpr), dataChunkToSelectPos, move(prevOperator),
        executionContext, mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalIntersectToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& logicalIntersect = (const LogicalIntersect&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(
        logicalOperator->getFirstChild(), mapperContext, executionContext);
    auto leftDataPos = mapperContext.getDataPos(logicalIntersect.getLeftNodeID());
    auto rightDataPos = mapperContext.getDataPos(logicalIntersect.getRightNodeID());
    return make_unique<Intersect>(leftDataPos, rightDataPos, move(prevOperator), executionContext,
        mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalProjectionToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& logicalProjection = (const LogicalProjection&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(
        logicalOperator->getFirstChild(), mapperContext, executionContext);
    vector<unique_ptr<ExpressionEvaluator>> expressionEvaluators;
    vector<DataPos> expressionsOutputPos;
    for (auto& expression : logicalProjection.expressionsToProject) {
        expressionEvaluators.push_back(expressionMapper.mapLogicalExpressionToPhysical(
            *expression, mapperContext, executionContext));
        expressionsOutputPos.push_back(mapperContext.getDataPos(expression->getUniqueName()));
        mapperContext.addComputedExpressions(expression->getUniqueName());
    }
    return make_unique<Projection>(move(expressionEvaluators), move(expressionsOutputPos),
        logicalProjection.discardedGroupsPos, move(prevOperator), executionContext,
        mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanNodePropertyToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& scanProperty = (const LogicalScanNodeProperty&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(
        logicalOperator->getFirstChild(), mapperContext, executionContext);
    auto inDataPos = mapperContext.getDataPos(scanProperty.nodeID);
    auto outDataPos = mapperContext.getDataPos(scanProperty.getPropertyExpressionName());
    mapperContext.addComputedExpressions(scanProperty.getPropertyExpressionName());
    auto& nodeStore = graph.getNodesStore();
    if (scanProperty.isUnstructuredProperty) {
        auto lists = nodeStore.getNodeUnstrPropertyLists(scanProperty.nodeLabel);
        return make_unique<ScanUnstructuredProperty>(inDataPos, outDataPos,
            scanProperty.getPropertyKey(), lists, move(prevOperator), executionContext,
            mapperContext.getOperatorID());
    }
    auto column =
        nodeStore.getNodePropertyColumn(scanProperty.nodeLabel, scanProperty.getPropertyKey());
    return make_unique<ScanStructuredProperty>(inDataPos, outDataPos, column, move(prevOperator),
        executionContext, mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanRelPropertyToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& scanProperty = (const LogicalScanRelProperty&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(
        logicalOperator->getFirstChild(), mapperContext, executionContext);
    auto inDataPos = mapperContext.getDataPos(scanProperty.boundNodeID);
    auto outDataPos = mapperContext.getDataPos(scanProperty.getPropertyExpressionName());
    mapperContext.addComputedExpressions(scanProperty.getPropertyExpressionName());
    auto& relStore = graph.getRelsStore();
    if (scanProperty.isColumn) {
        auto column = relStore.getRelPropertyColumn(
            scanProperty.relLabel, scanProperty.boundNodeLabel, scanProperty.getPropertyKey());
        return make_unique<ScanStructuredProperty>(inDataPos, outDataPos, column,
            move(prevOperator), executionContext, mapperContext.getOperatorID());
    }
    auto lists = relStore.getRelPropertyLists(scanProperty.direction, scanProperty.boundNodeLabel,
        scanProperty.relLabel, scanProperty.getPropertyKey());
    return make_unique<ReadRelPropertyList>(inDataPos, outDataPos, lists, move(prevOperator),
        executionContext, mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalHashJoinToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& hashJoin = (const LogicalHashJoin&)*logicalOperator;
    auto buildSideMapperContext =
        MapperContext(make_unique<ResultSetDescriptor>(*hashJoin.buildSideSchema));
    auto buildSidePrevOperator = mapLogicalOperatorToPhysical(
        hashJoin.getSecondChild(), buildSideMapperContext, executionContext);
    auto probeSidePrevOperator =
        mapLogicalOperatorToPhysical(hashJoin.getFirstChild(), mapperContext, executionContext);
    // Populate build side and probe side vector positions
    auto buildSideKeyIDDataPos = buildSideMapperContext.getDataPos(hashJoin.joinNodeID);
    auto probeSideKeyIDDataPos = mapperContext.getDataPos(hashJoin.joinNodeID);
    vector<bool> isBuildSideNonKeyDataFlat;
    vector<DataPos> buildSideNonKeyDataPoses;
    vector<DataPos> probeSideNonKeyDataPoses;
    auto& buildSideSchema = *hashJoin.buildSideSchema;
    for (auto& expressionName : hashJoin.getExpressionToMaterializeNames()) {
        if (expressionName == hashJoin.joinNodeID) {
            continue;
        }
        mapperContext.addComputedExpressions(expressionName);
        buildSideNonKeyDataPoses.push_back(buildSideMapperContext.getDataPos(expressionName));
        isBuildSideNonKeyDataFlat.push_back(buildSideSchema.getGroup(expressionName)->isFlat);
        probeSideNonKeyDataPoses.push_back(mapperContext.getDataPos(expressionName));
    }

    auto sharedState = make_shared<HashJoinSharedState>();
    // create hashJoin build
    auto buildDataInfo =
        BuildDataInfo(buildSideKeyIDDataPos, buildSideNonKeyDataPoses, isBuildSideNonKeyDataFlat);
    auto hashJoinBuild = make_unique<HashJoinBuild>(sharedState, buildDataInfo,
        move(buildSidePrevOperator), executionContext, mapperContext.getOperatorID());
    // create hashJoin probe
    auto probeDataInfo = ProbeDataInfo(probeSideKeyIDDataPos, probeSideNonKeyDataPoses);
    auto hashJoinProbe =
        make_unique<HashJoinProbe>(sharedState, probeDataInfo, move(probeSidePrevOperator),
            move(hashJoinBuild), executionContext, mapperContext.getOperatorID());
    return hashJoinProbe;
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalMultiplicityReducerToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto prevOperator = mapLogicalOperatorToPhysical(
        logicalOperator->getFirstChild(), mapperContext, executionContext);
    return make_unique<MultiplicityReducer>(
        move(prevOperator), executionContext, mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSkipToPhysical(LogicalOperator* logicalOperator,
    MapperContext& mapperContext, ExecutionContext& executionContext) {
    auto& logicalSkip = (const LogicalSkip&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(
        logicalOperator->getFirstChild(), mapperContext, executionContext);
    auto dataChunkToSelectPos = logicalSkip.getGroupPosToSelect();
    return make_unique<Skip>(logicalSkip.getSkipNumber(), make_shared<atomic_uint64_t>(0),
        dataChunkToSelectPos, logicalSkip.getGroupsPosToSkip(), move(prevOperator),
        executionContext, mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalLimitToPhysical(LogicalOperator* logicalOperator,
    MapperContext& mapperContext, ExecutionContext& executionContext) {
    auto& logicalLimit = (const LogicalLimit&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(
        logicalOperator->getFirstChild(), mapperContext, executionContext);
    auto dataChunkToSelectPos = logicalLimit.getGroupPosToSelect();
    return make_unique<Limit>(logicalLimit.getLimitNumber(), make_shared<atomic_uint64_t>(0),
        dataChunkToSelectPos, logicalLimit.getGroupsPosToLimit(), move(prevOperator),
        executionContext, mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalAggregateToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& logicalAggregate = (const LogicalAggregate&)*logicalOperator;
    auto mapperContextBeforeAggregate = MapperContext(
        make_unique<ResultSetDescriptor>(*logicalAggregate.getSchemaBeforeAggregate()));
    auto prevOperator = mapLogicalOperatorToPhysical(
        logicalOperator->getFirstChild(), mapperContextBeforeAggregate, executionContext);
    vector<DataType> aggregateDataTypes;
    vector<unique_ptr<AggregateFunction>> aggregateFunctions;
    vector<DataPos> aggregateVectorsInputPos;
    vector<DataPos> aggregateVectorsOutputPos;
    for (auto& expression : logicalAggregate.getExpressionsToAggregate()) {
        // Populate input info
        DataPos inputDataPos = DataPos{UINT32_MAX, UINT32_MAX};
        unique_ptr<AggregateFunction> aggregateFunction;
        if (expression->expressionType == COUNT_STAR_FUNC) {
            aggregateFunction = AggregateFunctionUtil::getCountStarFunction();
        } else {
            assert(expression->getChildren().size() == 1);
            auto aggregateChild = expression->getChild(0);
            inputDataPos = mapperContextBeforeAggregate.getDataPos(aggregateChild->getUniqueName());
            aggregateFunction = AggregateFunctionUtil::getAggregateFunction(
                expression->expressionType, aggregateChild->dataType);
        }
        aggregateVectorsInputPos.push_back(inputDataPos);
        aggregateFunctions.push_back(move(aggregateFunction));
        // Populate output info
        aggregateDataTypes.push_back(expression->dataType);
        aggregateVectorsOutputPos.push_back(mapperContext.getDataPos(expression->getUniqueName()));
        mapperContext.addComputedExpressions(expression->getUniqueName());
    }
    // Create the shared state.
    vector<unique_ptr<AggregateState>> aggregateStates;
    for (auto& aggregateFunction : aggregateFunctions) {
        aggregateStates.push_back(aggregateFunction->initialize());
    }
    auto sharedState = make_shared<AggregateSharedState>(move(aggregateStates));

    auto simpleAggregate = make_unique<SimpleAggregate>(sharedState, aggregateVectorsInputPos,
        move(aggregateFunctions), move(prevOperator), executionContext,
        mapperContext.getOperatorID());
    auto simpleAggregateScan = make_unique<SimpleAggregateScan>(
        mapperContext.getResultSetDescriptor()->copy(), sharedState, aggregateVectorsOutputPos,
        aggregateDataTypes, move(simpleAggregate), executionContext, mapperContext.getOperatorID());
    return simpleAggregateScan;
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalExistsToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& logicalExists = (LogicalExists&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(
        logicalOperator->getFirstChild(), mapperContext, executionContext);
    auto subPlanMapperContext =
        MapperContext(make_unique<ResultSetDescriptor>(*logicalExists.subPlanSchema));
    auto prevContext = enterSubquery(&mapperContext);
    auto subPlanLastOperator = mapLogicalOperatorToPhysical(
        logicalExists.getSecondChild(), subPlanMapperContext, executionContext);
    exitSubquery(prevContext);
    mapperContext.addComputedExpressions(logicalExists.subqueryExpression->getUniqueName());
    auto outDataPos = mapperContext.getDataPos(logicalExists.subqueryExpression->getUniqueName());
    return make_unique<Exists>(outDataPos, move(prevOperator), move(subPlanLastOperator),
        executionContext, mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalLeftNestedLoopJoinToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& logicalLeftNestedLoopJoin = (LogicalLeftNestedLoopJoin&)*logicalOperator;
    auto& subPlanSchema = *logicalLeftNestedLoopJoin.subPlanSchema;
    auto subPlanMapperContext = MapperContext(make_unique<ResultSetDescriptor>(subPlanSchema));
    auto prevOperator = mapLogicalOperatorToPhysical(
        logicalOperator->getFirstChild(), mapperContext, executionContext);
    auto prevMapperContext = enterSubquery(&mapperContext);
    auto subPlanLastOperator = mapLogicalOperatorToPhysical(
        logicalLeftNestedLoopJoin.getSecondChild(), subPlanMapperContext, executionContext);
    exitSubquery(prevMapperContext);
    vector<pair<DataPos, DataPos>> subPlanVectorsToRefPosMapping;
    // Populate data position mapping for merging sub-plan result set
    for (auto i = 0u; i < subPlanSchema.getNumGroups(); ++i) {
        auto& group = *subPlanSchema.getGroup(i);
        for (auto& expressionName : group.expressionNames) {
            if (mapperContext.expressionHasComputed(expressionName)) {
                continue;
            }
            auto innerPos = subPlanMapperContext.getDataPos(expressionName);
            auto outerPos = mapperContext.getDataPos(expressionName);
            subPlanVectorsToRefPosMapping.emplace_back(innerPos, outerPos);
            mapperContext.addComputedExpressions(expressionName);
        }
    }
    return make_unique<LeftNestedLoopJoin>(move(subPlanVectorsToRefPosMapping), move(prevOperator),
        move(subPlanLastOperator), executionContext, mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalOrderByToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& logicalOrderBy = (LogicalOrderBy&)*logicalOperator;
    auto& schemaBeforeOrderBy = *logicalOrderBy.getSchemaBeforeOrderBy();
    auto mapperContextBeforeOrderBy =
        MapperContext(make_unique<ResultSetDescriptor>(schemaBeforeOrderBy));
    auto orderByPrevOperator = mapLogicalOperatorToPhysical(
        logicalOrderBy.getFirstChild(), mapperContextBeforeOrderBy, executionContext);
    vector<DataPos> keyDataPoses;
    for (auto& expressionName : logicalOrderBy.getOrderByExpressionNames()) {
        keyDataPoses.emplace_back(mapperContextBeforeOrderBy.getDataPos(expressionName));
    }
    vector<DataPos> inputDataPoses;
    vector<bool> isInputVectorFlat;
    vector<DataPos> outputDataPoses;
    for (auto& expressionName : logicalOrderBy.getExpressionToMaterializeNames()) {
        inputDataPoses.push_back(mapperContextBeforeOrderBy.getDataPos(expressionName));
        isInputVectorFlat.push_back(schemaBeforeOrderBy.getGroup(expressionName)->isFlat);
        outputDataPoses.push_back(mapperContext.getDataPos(expressionName));
        mapperContext.addComputedExpressions(expressionName);
    }

    auto orderByDataInfo = OrderByDataInfo(
        keyDataPoses, inputDataPoses, isInputVectorFlat, logicalOrderBy.getIsAscOrders());
    auto orderBySharedState = make_shared<SharedFactorizedTablesAndSortedKeyBlocks>();

    auto orderBy = make_unique<OrderBy>(orderByDataInfo, orderBySharedState,
        move(orderByPrevOperator), executionContext, mapperContext.getOperatorID());
    auto orderByMerge = make_unique<OrderByMerge>(
        orderBySharedState, move(orderBy), executionContext, mapperContext.getOperatorID());
    auto orderByScan = make_unique<OrderByScan>(mapperContext.getResultSetDescriptor()->copy(),
        outputDataPoses, orderBySharedState, move(orderByMerge), executionContext,
        mapperContext.getOperatorID());
    return orderByScan;
}

} // namespace processor
} // namespace graphflow
