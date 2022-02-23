#include "src/processor/include/physical_plan/mapper/plan_mapper.h"

#include <set>

#include "src/function/include/aggregate/aggregate_function.h"
#include "src/planner/logical_plan/logical_operator/include/logical_aggregate.h"
#include "src/planner/logical_plan/logical_operator/include/logical_distinct.h"
#include "src/planner/logical_plan/logical_operator/include/logical_exist.h"
#include "src/planner/logical_plan/logical_operator/include/logical_extend.h"
#include "src/planner/logical_plan/logical_operator/include/logical_filter.h"
#include "src/planner/logical_plan/logical_operator/include/logical_flatten.h"
#include "src/planner/logical_plan/logical_operator/include/logical_hash_join.h"
#include "src/planner/logical_plan/logical_operator/include/logical_intersect.h"
#include "src/planner/logical_plan/logical_operator/include/logical_left_nested_loop_join.h"
#include "src/planner/logical_plan/logical_operator/include/logical_limit.h"
#include "src/planner/logical_plan/logical_operator/include/logical_order_by.h"
#include "src/planner/logical_plan/logical_operator/include/logical_projection.h"
#include "src/planner/logical_plan/logical_operator/include/logical_result_collector.h"
#include "src/planner/logical_plan/logical_operator/include/logical_result_scan.h"
#include "src/planner/logical_plan/logical_operator/include/logical_scan_node_id.h"
#include "src/planner/logical_plan/logical_operator/include/logical_scan_node_property.h"
#include "src/planner/logical_plan/logical_operator/include/logical_scan_rel_property.h"
#include "src/planner/logical_plan/logical_operator/include/logical_skip.h"
#include "src/planner/logical_plan/logical_operator/include/logical_union.h"
#include "src/processor/include/physical_plan/mapper/expression_mapper.h"
#include "src/processor/include/physical_plan/operator/aggregate/hash_aggregate.h"
#include "src/processor/include/physical_plan/operator/aggregate/hash_aggregate_scan.h"
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
#include "src/processor/include/physical_plan/operator/scan_column/adj_column_extend.h"
#include "src/processor/include/physical_plan/operator/scan_column/scan_structured_property.h"
#include "src/processor/include/physical_plan/operator/scan_column/scan_unstructured_property.h"
#include "src/processor/include/physical_plan/operator/scan_node_id.h"
#include "src/processor/include/physical_plan/operator/skip.h"
#include "src/processor/include/physical_plan/operator/union_all_scan.h"

using namespace graphflow::planner;

namespace graphflow {
namespace processor {

static unique_ptr<PhysicalOperator> createHashAggregate(
    vector<unique_ptr<AggregateFunction>> aggregateFunctions, vector<DataPos> inputAggVectorsPos,
    vector<DataPos> outputAggVectorsPos, vector<DataType> outputAggVectorsDataType,
    const expression_vector& groupByExpressions, unique_ptr<PhysicalOperator> prevOperator,
    MapperContext& mapperContextBeforeAggregate, MapperContext& mapperContext,
    ExecutionContext& executionContext);

unique_ptr<PhysicalPlan> PlanMapper::mapLogicalPlanToPhysical(
    unique_ptr<LogicalPlan> logicalPlan, ExecutionContext& executionContext) {
    auto mapperContext = MapperContext(make_unique<ResultSetDescriptor>(*logicalPlan->getSchema()));
    auto resultCollector = mapLogicalOperatorToPhysical(
        logicalPlan->getLastOperator(), mapperContext, executionContext);
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
    case LOGICAL_DISTINCT: {
        physicalOperator =
            mapLogicalDistinctToPhysical(logicalOperator.get(), mapperContext, executionContext);
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
    case LOGICAL_UNION_ALL: {
        physicalOperator =
            mapLogicalUnionAllToPhysical(logicalOperator.get(), mapperContext, executionContext);
    } break;
    case LOGICAL_RESULT_COLLECTOR: {
        physicalOperator = mapLogicalResultCollectorToPhysical(
            logicalOperator.get(), mapperContext, executionContext);
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
    auto sharedState = make_shared<ScanNodeIDSharedState>(graph.getNumNodes(logicalScan.label));
    auto dataPos = mapperContext.getDataPos(logicalScan.nodeID);
    mapperContext.addComputedExpressions(logicalScan.nodeID);
    return make_unique<ScanNodeID>(mapperContext.getResultSetDescriptor()->copy(),
        logicalScan.label, dataPos, sharedState, executionContext, mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalResultScanToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& resultScan = (const LogicalResultScan&)*logicalOperator;
    auto expressionsToScan = resultScan.getExpressionsToScan();
    assert(!expressionsToScan.empty());
    vector<DataPos> inDataPoses;
    uint32_t outDataChunkPos =
        mapperContext.getDataPos(expressionsToScan[0]->getUniqueName()).dataChunkPos;
    vector<uint32_t> outValueVectorsPos;
    for (auto& expression : expressionsToScan) {
        auto expressionName = expression->getUniqueName();
        inDataPoses.push_back(outerMapperContext->getDataPos(expressionName));
        // all variables should be appended to the same datachunk
        assert(outDataChunkPos == mapperContext.getDataPos(expressionName).dataChunkPos);
        outValueVectorsPos.push_back(mapperContext.getDataPos(expressionName).valueVectorPos);
        mapperContext.addComputedExpressions(expressionName);
    }
    return make_unique<ResultScan>(mapperContext.getResultSetDescriptor()->copy(),
        move(inDataPoses), outDataChunkPos, move(outValueVectorsPos), executionContext,
        mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalExtendToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& extend = (const LogicalExtend&)*logicalOperator;
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext, executionContext);
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
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext, executionContext);
    auto dataChunkPos =
        mapperContext.getDataPos(flatten.getExpressionToFlatten()->getUniqueName()).dataChunkPos;
    return make_unique<Flatten>(
        dataChunkPos, move(prevOperator), executionContext, mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalFilterToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& logicalFilter = (const LogicalFilter&)*logicalOperator;
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext, executionContext);
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
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext, executionContext);
    auto leftDataPos = mapperContext.getDataPos(logicalIntersect.getLeftNodeID());
    auto rightDataPos = mapperContext.getDataPos(logicalIntersect.getRightNodeID());
    return make_unique<Intersect>(leftDataPos, rightDataPos, move(prevOperator), executionContext,
        mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalProjectionToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& logicalProjection = (const LogicalProjection&)*logicalOperator;
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext, executionContext);
    vector<unique_ptr<ExpressionEvaluator>> expressionEvaluators;
    vector<DataPos> expressionsOutputPos;
    for (auto& expression : logicalProjection.getExpressionsToProject()) {
        expressionEvaluators.push_back(expressionMapper.mapLogicalExpressionToPhysical(
            *expression, mapperContext, executionContext));
        expressionsOutputPos.push_back(mapperContext.getDataPos(expression->getUniqueName()));
        mapperContext.addComputedExpressions(expression->getUniqueName());
    }
    return make_unique<Projection>(move(expressionEvaluators), move(expressionsOutputPos),
        logicalProjection.getDiscardedGroupsPos(), move(prevOperator), executionContext,
        mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanNodePropertyToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& scanProperty = (const LogicalScanNodeProperty&)*logicalOperator;
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext, executionContext);
    auto inputNodeIDVectorPos = mapperContext.getDataPos(scanProperty.getNodeID());
    vector<DataPos> outputPropertyVectorsPos;
    for (auto& propertyName : scanProperty.getPropertyNames()) {
        outputPropertyVectorsPos.push_back(mapperContext.getDataPos(propertyName));
        mapperContext.addComputedExpressions(propertyName);
    }
    auto& nodeStore = graph.getNodesStore();
    if (scanProperty.getIsUnstructured()) {
        auto lists = nodeStore.getNodeUnstrPropertyLists(scanProperty.getNodeLabel());
        return make_unique<ScanUnstructuredProperty>(inputNodeIDVectorPos,
            move(outputPropertyVectorsPos), scanProperty.getPropertyKeys(), lists,
            move(prevOperator), executionContext, mapperContext.getOperatorID());
    }
    vector<Column*> propertyColumns;
    for (auto& propertyKey : scanProperty.getPropertyKeys()) {
        propertyColumns.push_back(
            nodeStore.getNodePropertyColumn(scanProperty.getNodeLabel(), propertyKey));
    }
    return make_unique<ScanStructuredProperty>(inputNodeIDVectorPos, move(outputPropertyVectorsPos),
        move(propertyColumns), move(prevOperator), executionContext, mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanRelPropertyToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& scanRelProperty = (const LogicalScanRelProperty&)*logicalOperator;
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext, executionContext);
    auto inputNodeIDVectorPos = mapperContext.getDataPos(scanRelProperty.getBoundNodeID());
    auto propertyName = scanRelProperty.getPropertyName();
    auto propertyKey = scanRelProperty.getPropertyKey();
    auto outputPropertyVectorPos = mapperContext.getDataPos(propertyName);
    mapperContext.addComputedExpressions(propertyName);
    auto& relStore = graph.getRelsStore();
    if (scanRelProperty.getIsColumn()) {
        auto column = relStore.getRelPropertyColumn(
            scanRelProperty.getRelLabel(), scanRelProperty.getBoundNodeLabel(), propertyKey);
        return make_unique<ScanStructuredProperty>(inputNodeIDVectorPos,
            vector<DataPos>{outputPropertyVectorPos}, vector<Column*>{column}, move(prevOperator),
            executionContext, mapperContext.getOperatorID());
    }
    auto lists = relStore.getRelPropertyLists(scanRelProperty.getDirection(),
        scanRelProperty.getBoundNodeLabel(), scanRelProperty.getRelLabel(), propertyKey);
    return make_unique<ReadRelPropertyList>(inputNodeIDVectorPos, move(outputPropertyVectorPos),
        lists, move(prevOperator), executionContext, mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalHashJoinToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& hashJoin = (const LogicalHashJoin&)*logicalOperator;
    auto buildSideMapperContext =
        MapperContext(make_unique<ResultSetDescriptor>(*hashJoin.getBuildSideSchema()));
    auto buildSidePrevOperator = mapLogicalOperatorToPhysical(
        hashJoin.getChild(1), buildSideMapperContext, executionContext);
    auto probeSidePrevOperator =
        mapLogicalOperatorToPhysical(hashJoin.getChild(0), mapperContext, executionContext);
    // Populate build side and probe side vector positions
    auto buildSideKeyIDDataPos = buildSideMapperContext.getDataPos(hashJoin.getJoinNodeID());
    auto probeSideKeyIDDataPos = mapperContext.getDataPos(hashJoin.getJoinNodeID());
    vector<bool> isBuildSideNonKeyDataFlat;
    vector<DataPos> buildSideNonKeyDataPoses;
    vector<DataPos> probeSideNonKeyDataPoses;
    auto& buildSideSchema = *hashJoin.getBuildSideSchema();
    for (auto& expression : hashJoin.getExpressionsToMaterialize()) {
        auto expressionName = expression->getUniqueName();
        if (expressionName == hashJoin.getJoinNodeID()) {
            continue;
        }
        mapperContext.addComputedExpressions(expressionName);
        buildSideNonKeyDataPoses.push_back(buildSideMapperContext.getDataPos(expressionName));
        isBuildSideNonKeyDataFlat.push_back(buildSideSchema.getGroup(expressionName)->getIsFlat());
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
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext, executionContext);
    return make_unique<MultiplicityReducer>(
        move(prevOperator), executionContext, mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSkipToPhysical(LogicalOperator* logicalOperator,
    MapperContext& mapperContext, ExecutionContext& executionContext) {
    auto& logicalSkip = (const LogicalSkip&)*logicalOperator;
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext, executionContext);
    auto dataChunkToSelectPos = logicalSkip.getGroupPosToSelect();
    return make_unique<Skip>(logicalSkip.getSkipNumber(), make_shared<atomic_uint64_t>(0),
        dataChunkToSelectPos, logicalSkip.getGroupsPosToSkip(), move(prevOperator),
        executionContext, mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalLimitToPhysical(LogicalOperator* logicalOperator,
    MapperContext& mapperContext, ExecutionContext& executionContext) {
    auto& logicalLimit = (const LogicalLimit&)*logicalOperator;
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext, executionContext);
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
        logicalOperator->getChild(0), mapperContextBeforeAggregate, executionContext);
    vector<DataPos> inputAggVectorsPos;
    vector<DataPos> outputAggVectorsPos;
    vector<DataType> outputAggVectorsDataType;
    vector<unique_ptr<AggregateFunction>> aggregateFunctions;
    for (auto& expression : logicalAggregate.getExpressionsToAggregate()) {
        if (expression->expressionType == COUNT_STAR_FUNC) {
            inputAggVectorsPos.emplace_back(UINT32_MAX, UINT32_MAX);
        } else {
            auto child = expression->getChild(0);
            inputAggVectorsPos.push_back(
                mapperContextBeforeAggregate.getDataPos(child->getUniqueName()));
        }
        aggregateFunctions.push_back(AggregateFunctionUtil::getAggregateFunction(*expression));
        outputAggVectorsPos.push_back(mapperContext.getDataPos(expression->getUniqueName()));
        outputAggVectorsDataType.push_back(expression->dataType);
        mapperContext.addComputedExpressions(expression->getUniqueName());
    }
    if (logicalAggregate.hasExpressionsToGroupBy()) {
        return createHashAggregate(move(aggregateFunctions), inputAggVectorsPos,
            outputAggVectorsPos, outputAggVectorsDataType,
            logicalAggregate.getExpressionsToGroupBy(), move(prevOperator),
            mapperContextBeforeAggregate, mapperContext, executionContext);
    } else {
        auto sharedState = make_shared<SimpleAggregateSharedState>(aggregateFunctions);
        auto aggregate =
            make_unique<SimpleAggregate>(sharedState, inputAggVectorsPos, move(aggregateFunctions),
                move(prevOperator), executionContext, mapperContext.getOperatorID());
        auto aggregateScan = make_unique<SimpleAggregateScan>(sharedState,
            mapperContext.getResultSetDescriptor()->copy(), outputAggVectorsPos,
            outputAggVectorsDataType, move(aggregate), executionContext,
            mapperContext.getOperatorID());
        return aggregateScan;
    }
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalDistinctToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& logicalDistinct = (const LogicalDistinct&)*logicalOperator;
    auto mapperContextBeforeDistinct =
        MapperContext(make_unique<ResultSetDescriptor>(*logicalDistinct.getSchemaBeforeDistinct()));
    auto prevOperator = mapLogicalOperatorToPhysical(
        logicalOperator->getChild(0), mapperContextBeforeDistinct, executionContext);
    vector<unique_ptr<AggregateFunction>> emptyAggregateFunctions;
    vector<DataPos> emptyInputAggVectorsPos;
    vector<DataPos> emptyOutputAggVectorsPos;
    vector<DataType> emptyOutputAggVectorsDataType;
    return createHashAggregate(move(emptyAggregateFunctions), emptyInputAggVectorsPos,
        emptyOutputAggVectorsPos, emptyOutputAggVectorsDataType,
        logicalDistinct.getExpressionsToDistinct(), move(prevOperator), mapperContextBeforeDistinct,
        mapperContext, executionContext);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalExistsToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& logicalExists = (LogicalExists&)*logicalOperator;
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext, executionContext);
    auto subPlanMapperContext =
        MapperContext(make_unique<ResultSetDescriptor>(*logicalExists.subPlanSchema));
    auto prevContext = enterSubquery(&mapperContext);
    auto subPlanLastOperator = mapLogicalOperatorToPhysical(
        logicalExists.getChild(1), subPlanMapperContext, executionContext);
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
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext, executionContext);
    auto prevMapperContext = enterSubquery(&mapperContext);
    auto subPlanLastOperator = mapLogicalOperatorToPhysical(
        logicalLeftNestedLoopJoin.getChild(1), subPlanMapperContext, executionContext);
    exitSubquery(prevMapperContext);
    vector<pair<DataPos, DataPos>> subPlanVectorsToRefPosMapping;
    // Populate data position mapping for merging sub-plan result set
    for (auto i = 0u; i < subPlanSchema.getNumGroups(); ++i) {
        auto& group = *subPlanSchema.getGroup(i);
        for (auto& expression : group.getExpressions()) {
            auto expressionName = expression->getUniqueName();
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
        logicalOrderBy.getChild(0), mapperContextBeforeOrderBy, executionContext);
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
        move(orderByPrevOperator), executionContext, mapperContext.getOperatorID());
    auto orderByMerge = make_unique<OrderByMerge>(
        orderBySharedState, move(orderBy), executionContext, mapperContext.getOperatorID());
    auto orderByScan = make_unique<OrderByScan>(mapperContext.getResultSetDescriptor()->copy(),
        outputDataPoses, orderBySharedState, move(orderByMerge), executionContext,
        mapperContext.getOperatorID());
    return orderByScan;
}

unique_ptr<ResultCollector> PlanMapper::mapLogicalResultCollectorToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& resultCollector = (LogicalResultCollector&)*logicalOperator;
    auto resultCollectorMapperContext =
        MapperContext(make_unique<ResultSetDescriptor>(*resultCollector.getSchema()));
    auto prevOperator = mapLogicalOperatorToPhysical(
        logicalOperator->getChild(0), resultCollectorMapperContext, executionContext);

    vector<pair<DataPos, bool>> valueVectorsToCollectInfo;
    for (auto& expression : resultCollector.getExpressionsToCollect()) {
        valueVectorsToCollectInfo.push_back(
            make_pair(resultCollectorMapperContext.getDataPos(expression->getUniqueName()),
                resultCollector.getSchema()->getGroup(expression->getUniqueName())->getIsFlat()));
    }
    return make_unique<ResultCollector>(valueVectorsToCollectInfo,
        make_shared<SharedQueryResults>(), move(prevOperator), executionContext,
        mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalUnionAllToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    auto& unionAll = (LogicalUnion&)*logicalOperator;
    vector<DataType> dataTypes;
    vector<DataPos> outDataPoses;
    for (auto& expression : unionAll.getExpressionsToUnion()) {
        outDataPoses.emplace_back(mapperContext.getDataPos(expression->getUniqueName()));
        dataTypes.push_back(expression->getDataType());
    }
    vector<unique_ptr<PhysicalOperator>> children;
    for (auto i = 0u; i < logicalOperator->getNumChildren(); i++) {
        children.emplace_back(mapLogicalResultCollectorToPhysical(
            logicalOperator->getChild(i).get(), mapperContext, executionContext));
    }
    return make_unique<UnionAllScan>(mapperContext.getResultSetDescriptor()->copy(),
        move(outDataPoses), move(dataTypes), move(children), executionContext,
        mapperContext.getOperatorID());
}

unique_ptr<PhysicalOperator> createHashAggregate(
    vector<unique_ptr<AggregateFunction>> aggregateFunctions, vector<DataPos> inputAggVectorsPos,
    vector<DataPos> outputAggVectorsPos, vector<DataType> outputAggVectorsDataType,
    const expression_vector& groupByExpressions, unique_ptr<PhysicalOperator> prevOperator,
    MapperContext& mapperContextBeforeAggregate, MapperContext& mapperContext,
    ExecutionContext& executionContext) {
    vector<DataPos> inputGroupByKeyVectorsPos;
    vector<DataPos> outputGroupByKeyVectorsPos;
    vector<DataType> outputGroupByKeyVectorsDataType;
    for (auto& expression : groupByExpressions) {
        inputGroupByKeyVectorsPos.push_back(
            mapperContextBeforeAggregate.getDataPos(expression->getUniqueName()));
        outputGroupByKeyVectorsPos.push_back(mapperContext.getDataPos(expression->getUniqueName()));
        outputGroupByKeyVectorsDataType.push_back(expression->dataType);
        mapperContext.addComputedExpressions(expression->getUniqueName());
    }
    auto sharedState = make_shared<HashAggregateSharedState>(aggregateFunctions);
    auto aggregate = make_unique<HashAggregate>(sharedState, inputGroupByKeyVectorsPos,
        move(inputAggVectorsPos), move(aggregateFunctions), move(prevOperator), executionContext,
        mapperContext.getOperatorID());
    auto aggregateScan = make_unique<HashAggregateScan>(sharedState,
        mapperContext.getResultSetDescriptor()->copy(), outputGroupByKeyVectorsPos,
        outputGroupByKeyVectorsDataType, move(outputAggVectorsPos), move(outputAggVectorsDataType),
        move(aggregate), executionContext, mapperContext.getOperatorID());
    return aggregateScan;
}

} // namespace processor
} // namespace graphflow
