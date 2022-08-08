#include "src/processor/include/physical_plan/mapper/plan_mapper.h"

#include <set>

#include "src/binder/expression/include/function_expression.h"
#include "src/binder/expression/include/property_expression.h"
#include "src/function/aggregate/include/aggregate_function.h"
#include "src/planner/logical_plan/logical_operator/include/logical_aggregate.h"
#include "src/planner/logical_plan/logical_operator/include/logical_copy_csv.h"
#include "src/planner/logical_plan/logical_operator/include/logical_create_node_table.h"
#include "src/planner/logical_plan/logical_operator/include/logical_create_rel_table.h"
#include "src/planner/logical_plan/logical_operator/include/logical_distinct.h"
#include "src/planner/logical_plan/logical_operator/include/logical_exist.h"
#include "src/planner/logical_plan/logical_operator/include/logical_filter.h"
#include "src/planner/logical_plan/logical_operator/include/logical_flatten.h"
#include "src/planner/logical_plan/logical_operator/include/logical_intersect.h"
#include "src/planner/logical_plan/logical_operator/include/logical_left_nested_loop_join.h"
#include "src/planner/logical_plan/logical_operator/include/logical_limit.h"
#include "src/planner/logical_plan/logical_operator/include/logical_order_by.h"
#include "src/planner/logical_plan/logical_operator/include/logical_projection.h"
#include "src/planner/logical_plan/logical_operator/include/logical_result_scan.h"
#include "src/planner/logical_plan/logical_operator/include/logical_scan_node_property.h"
#include "src/planner/logical_plan/logical_operator/include/logical_skip.h"
#include "src/processor/include/physical_plan/mapper/expression_mapper.h"
#include "src/processor/include/physical_plan/operator/aggregate/hash_aggregate.h"
#include "src/processor/include/physical_plan/operator/aggregate/hash_aggregate_scan.h"
#include "src/processor/include/physical_plan/operator/aggregate/simple_aggregate.h"
#include "src/processor/include/physical_plan/operator/aggregate/simple_aggregate_scan.h"
#include "src/processor/include/physical_plan/operator/copy_csv/copy_node_csv.h"
#include "src/processor/include/physical_plan/operator/create_node_table.h"
#include "src/processor/include/physical_plan/operator/create_rel_table.h"
#include "src/processor/include/physical_plan/operator/exists.h"
#include "src/processor/include/physical_plan/operator/filter.h"
#include "src/processor/include/physical_plan/operator/flatten.h"
#include "src/processor/include/physical_plan/operator/intersect.h"
#include "src/processor/include/physical_plan/operator/left_nested_loop_join.h"
#include "src/processor/include/physical_plan/operator/limit.h"
#include "src/processor/include/physical_plan/operator/multiplicity_reducer.h"
#include "src/processor/include/physical_plan/operator/order_by/order_by.h"
#include "src/processor/include/physical_plan/operator/order_by/order_by_merge.h"
#include "src/processor/include/physical_plan/operator/order_by/order_by_scan.h"
#include "src/processor/include/physical_plan/operator/projection.h"
#include "src/processor/include/physical_plan/operator/result_collector.h"
#include "src/processor/include/physical_plan/operator/result_scan.h"
#include "src/processor/include/physical_plan/operator/scan_column/scan_structured_property.h"
#include "src/processor/include/physical_plan/operator/scan_column/scan_unstructured_property.h"
#include "src/processor/include/physical_plan/operator/skip.h"

using namespace graphflow::planner;

namespace graphflow {
namespace processor {

unique_ptr<PhysicalPlan> PlanMapper::mapLogicalPlanToPhysical(unique_ptr<LogicalPlan> logicalPlan) {
    auto mapperContext = MapperContext(make_unique<ResultSetDescriptor>(*logicalPlan->getSchema()));
    auto prevOperator = mapLogicalOperatorToPhysical(logicalPlan->getLastOperator(), mapperContext);
    auto lastOperator = appendResultCollector(logicalPlan->getExpressionsToCollect(),
        *logicalPlan->getSchema(), move(prevOperator), mapperContext);
    return make_unique<PhysicalPlan>(move(lastOperator), logicalPlan->isReadOnly());
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
    const shared_ptr<LogicalOperator>& logicalOperator, MapperContext& mapperContext) {
    unique_ptr<PhysicalOperator> physicalOperator;
    auto operatorType = logicalOperator->getLogicalOperatorType();
    switch (operatorType) {
    case LOGICAL_SCAN_NODE_ID: {
        physicalOperator = mapLogicalScanNodeIDToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_SELECT_SCAN: {
        physicalOperator = mapLogicalResultScanToPhysical(logicalOperator.get(), mapperContext);
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
    case LOGICAL_INTERSECT: {
        physicalOperator = mapLogicalIntersectToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_PROJECTION: {
        physicalOperator = mapLogicalProjectionToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_HASH_JOIN: {
        physicalOperator = mapLogicalHashJoinToPhysical(logicalOperator.get(), mapperContext);
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
    case LOGICAL_EXISTS: {
        physicalOperator = mapLogicalExistsToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_LEFT_NESTED_LOOP_JOIN: {
        physicalOperator =
            mapLogicalLeftNestedLoopJoinToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_ORDER_BY: {
        physicalOperator = mapLogicalOrderByToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_UNION_ALL: {
        physicalOperator = mapLogicalUnionAllToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_SINK: {
        physicalOperator = mapLogicalSinkToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_STATIC_TABLE_SCAN: {
        physicalOperator = mapLogicalTableScanToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_CREATE: {
        physicalOperator = mapLogicalCreateToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_SET: {
        physicalOperator = mapLogicalSetToPhysical(logicalOperator.get(), mapperContext);
    } break;
    case LOGICAL_DELETE: {
        physicalOperator = mapLogicalDeleteToPhysical(logicalOperator.get(), mapperContext);
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
    default:
        assert(false);
    }
    return physicalOperator;
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalResultScanToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
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
        move(inDataPoses), outDataChunkPos, move(outValueVectorsPos), getOperatorID(),
        resultScan.getExpressionsForPrinting());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalFlattenToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& flatten = (const LogicalFlatten&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto dataChunkPos =
        mapperContext.getDataPos(flatten.getExpressionToFlatten()->getUniqueName()).dataChunkPos;
    return make_unique<Flatten>(
        dataChunkPos, move(prevOperator), getOperatorID(), flatten.getExpressionsForPrinting());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalFilterToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& logicalFilter = (const LogicalFilter&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto dataChunkToSelectPos = logicalFilter.groupPosToSelect;
    auto physicalRootExpr = expressionMapper.mapExpression(logicalFilter.expression, mapperContext);
    return make_unique<Filter>(move(physicalRootExpr), dataChunkToSelectPos, move(prevOperator),
        getOperatorID(), logicalFilter.getExpressionsForPrinting());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalIntersectToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& logicalIntersect = (const LogicalIntersect&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto leftDataPos = mapperContext.getDataPos(logicalIntersect.getLeftNodeID());
    auto rightDataPos = mapperContext.getDataPos(logicalIntersect.getRightNodeID());
    return make_unique<Intersect>(leftDataPos, rightDataPos, move(prevOperator), getOperatorID(),
        logicalIntersect.getExpressionsForPrinting());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalProjectionToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& logicalProjection = (const LogicalProjection&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    vector<unique_ptr<BaseExpressionEvaluator>> expressionEvaluators;
    vector<DataPos> expressionsOutputPos;
    for (auto& expression : logicalProjection.getExpressionsToProject()) {
        expressionEvaluators.push_back(expressionMapper.mapExpression(expression, mapperContext));
        expressionsOutputPos.push_back(mapperContext.getDataPos(expression->getUniqueName()));
        mapperContext.addComputedExpressions(expression->getUniqueName());
    }
    return make_unique<Projection>(move(expressionEvaluators), move(expressionsOutputPos),
        logicalProjection.getDiscardedGroupsPos(), move(prevOperator), getOperatorID(),
        logicalProjection.getExpressionsForPrinting());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanNodePropertyToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& scanProperty = (const LogicalScanNodeProperty&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto inputNodeIDVectorPos = mapperContext.getDataPos(scanProperty.getNodeID());
    auto paramsString = scanProperty.getExpressionsForPrinting();
    vector<DataPos> outputPropertyVectorsPos;
    for (auto& propertyName : scanProperty.getPropertyNames()) {
        outputPropertyVectorsPos.push_back(mapperContext.getDataPos(propertyName));
        mapperContext.addComputedExpressions(propertyName);
    }
    auto& nodeStore = storageManager.getNodesStore();
    if (scanProperty.getIsUnstructured()) {
        auto lists = nodeStore.getNodeUnstrPropertyLists(scanProperty.getNodeLabel());
        return make_unique<ScanUnstructuredProperty>(inputNodeIDVectorPos,
            move(outputPropertyVectorsPos), scanProperty.getPropertyKeys(), lists,
            move(prevOperator), getOperatorID(), paramsString);
    }
    vector<Column*> propertyColumns;
    for (auto& propertyKey : scanProperty.getPropertyKeys()) {
        propertyColumns.push_back(
            nodeStore.getNodePropertyColumn(scanProperty.getNodeLabel(), propertyKey));
    }
    return make_unique<ScanStructuredProperty>(inputNodeIDVectorPos, move(outputPropertyVectorsPos),
        move(propertyColumns), move(prevOperator), getOperatorID(), paramsString);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalMultiplicityReducerToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    return make_unique<MultiplicityReducer>(
        move(prevOperator), getOperatorID(), logicalOperator->getExpressionsForPrinting());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSkipToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& logicalSkip = (const LogicalSkip&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto dataChunkToSelectPos = logicalSkip.getGroupPosToSelect();
    return make_unique<Skip>(logicalSkip.getSkipNumber(), make_shared<atomic_uint64_t>(0),
        dataChunkToSelectPos, logicalSkip.getGroupsPosToSkip(), move(prevOperator), getOperatorID(),
        logicalSkip.getExpressionsForPrinting());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalLimitToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& logicalLimit = (const LogicalLimit&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto dataChunkToSelectPos = logicalLimit.getGroupPosToSelect();
    return make_unique<Limit>(logicalLimit.getLimitNumber(), make_shared<atomic_uint64_t>(0),
        dataChunkToSelectPos, logicalLimit.getGroupsPosToLimit(), move(prevOperator),
        getOperatorID(), logicalLimit.getExpressionsForPrinting());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalAggregateToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& logicalAggregate = (const LogicalAggregate&)*logicalOperator;
    auto mapperContextBeforeAggregate = MapperContext(
        make_unique<ResultSetDescriptor>(*logicalAggregate.getSchemaBeforeAggregate()));
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContextBeforeAggregate);
    auto paramsString = logicalAggregate.getExpressionsForPrinting();
    vector<DataPos> inputAggVectorsPos;
    vector<DataPos> outputAggVectorsPos;
    vector<DataType> outputAggVectorsDataTypes;
    vector<unique_ptr<AggregateFunction>> aggregateFunctions;
    for (auto& expression : logicalAggregate.getExpressionsToAggregate()) {
        if (expression->getNumChildren() == 0) {
            inputAggVectorsPos.emplace_back(UINT32_MAX, UINT32_MAX);
        } else {
            auto child = expression->getChild(0);
            inputAggVectorsPos.push_back(
                mapperContextBeforeAggregate.getDataPos(child->getUniqueName()));
        }

        aggregateFunctions.push_back(
            ((AggregateFunctionExpression&)*expression).aggregateFunction->clone());
        outputAggVectorsPos.push_back(mapperContext.getDataPos(expression->getUniqueName()));
        outputAggVectorsDataTypes.push_back(expression->dataType);
        mapperContext.addComputedExpressions(expression->getUniqueName());
    }
    if (logicalAggregate.hasExpressionsToGroupBy()) {
        return createHashAggregate(move(aggregateFunctions), inputAggVectorsPos,
            outputAggVectorsPos, outputAggVectorsDataTypes,
            logicalAggregate.getExpressionsToGroupBy(), logicalAggregate.getSchemaBeforeAggregate(),
            move(prevOperator), mapperContextBeforeAggregate, mapperContext, paramsString);
    } else {
        auto sharedState = make_shared<SimpleAggregateSharedState>(aggregateFunctions);
        auto aggregate = make_unique<SimpleAggregate>(sharedState, inputAggVectorsPos,
            move(aggregateFunctions), move(prevOperator), getOperatorID(), paramsString);
        auto aggregateScan = make_unique<SimpleAggregateScan>(sharedState,
            mapperContext.getResultSetDescriptor()->copy(), outputAggVectorsPos,
            outputAggVectorsDataTypes, move(aggregate), getOperatorID(), paramsString);
        return aggregateScan;
    }
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalDistinctToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& logicalDistinct = (const LogicalDistinct&)*logicalOperator;
    auto mapperContextBeforeDistinct =
        MapperContext(make_unique<ResultSetDescriptor>(*logicalDistinct.getSchemaBeforeDistinct()));
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContextBeforeDistinct);
    vector<unique_ptr<AggregateFunction>> emptyAggregateFunctions;
    vector<DataPos> emptyInputAggVectorsPos;
    vector<DataPos> emptyOutputAggVectorsPos;
    vector<DataType> emptyOutputAggVectorsDataTypes;
    return createHashAggregate(move(emptyAggregateFunctions), emptyInputAggVectorsPos,
        emptyOutputAggVectorsPos, emptyOutputAggVectorsDataTypes,
        logicalDistinct.getExpressionsToDistinct(), logicalDistinct.getSchemaBeforeDistinct(),
        move(prevOperator), mapperContextBeforeDistinct, mapperContext,
        logicalDistinct.getExpressionsForPrinting());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalExistsToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& logicalExists = (LogicalExists&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto subPlanMapperContext =
        MapperContext(make_unique<ResultSetDescriptor>(*logicalExists.subPlanSchema));
    auto prevContext = enterSubquery(&mapperContext);
    auto subPlanLastOperator =
        mapLogicalOperatorToPhysical(logicalExists.getChild(1), subPlanMapperContext);
    exitSubquery(prevContext);
    mapperContext.addComputedExpressions(logicalExists.subqueryExpression->getUniqueName());
    auto outDataPos = mapperContext.getDataPos(logicalExists.subqueryExpression->getUniqueName());
    return make_unique<Exists>(outDataPos, move(prevOperator), move(subPlanLastOperator),
        getOperatorID(), logicalExists.getExpressionsForPrinting());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalLeftNestedLoopJoinToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& logicalLeftNestedLoopJoin = (LogicalLeftNestedLoopJoin&)*logicalOperator;
    auto& subPlanSchema = *logicalLeftNestedLoopJoin.subPlanSchema;
    auto subPlanMapperContext = MapperContext(make_unique<ResultSetDescriptor>(subPlanSchema));
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto prevMapperContext = enterSubquery(&mapperContext);
    auto subPlanLastOperator =
        mapLogicalOperatorToPhysical(logicalLeftNestedLoopJoin.getChild(1), subPlanMapperContext);
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
        move(subPlanLastOperator), getOperatorID(),
        logicalLeftNestedLoopJoin.getExpressionsForPrinting());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalOrderByToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& logicalOrderBy = (LogicalOrderBy&)*logicalOperator;
    auto& schemaBeforeOrderBy = *logicalOrderBy.getSchemaBeforeOrderBy();
    auto mapperContextBeforeOrderBy =
        MapperContext(make_unique<ResultSetDescriptor>(schemaBeforeOrderBy));
    auto orderByPrevOperator =
        mapLogicalOperatorToPhysical(logicalOrderBy.getChild(0), mapperContextBeforeOrderBy);
    auto paramsString = logicalOrderBy.getExpressionsForPrinting();
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
        move(orderByPrevOperator), getOperatorID(), paramsString);
    auto orderByMerge =
        make_unique<OrderByMerge>(orderBySharedState, move(orderBy), getOperatorID(), paramsString);
    auto orderByScan = make_unique<OrderByScan>(mapperContext.getResultSetDescriptor()->copy(),
        outputDataPoses, orderBySharedState, move(orderByMerge), getOperatorID(), paramsString);
    return orderByScan;
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCreateNodeTableToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& createNodeTable = (LogicalCreateNodeTable&)*logicalOperator;
    return make_unique<CreateNodeTable>(catalog, createNodeTable.getLabelName(),
        createNodeTable.getPropertyNameDataTypes(), createNodeTable.getPrimaryKeyIdx(),
        getOperatorID(), createNodeTable.getExpressionsForPrinting(),
        &storageManager.getNodesStore().getNodesMetadata());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCreateRelTableToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& createRelTable = (LogicalCreateRelTable&)*logicalOperator;
    return make_unique<CreateRelTable>(catalog, createRelTable.getLabelName(),
        createRelTable.getPropertyNameDataTypes(), createRelTable.getRelMultiplicity(),
        createRelTable.getSrcDstLabels(), getOperatorID(),
        createRelTable.getExpressionsForPrinting());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCopyCSVToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& copyCSV = (LogicalCopyCSV&)*logicalOperator;
    return make_unique<CopyNodeCSV>(catalog, copyCSV.getCSVDescription(),
        storageManager.getDirectory(), &storageManager.getNodesStore().getNodesMetadata(),
        bufferManager, storageManager.getWAL(), getOperatorID(),
        copyCSV.getExpressionsForPrinting());
}

unique_ptr<ResultCollector> PlanMapper::appendResultCollector(
    expression_vector expressionsToCollect, const Schema& schema,
    unique_ptr<PhysicalOperator> prevOperator, MapperContext& mapperContext) {
    vector<pair<DataPos, bool>> valueVectorsToCollectInfo;
    for (auto& expression : expressionsToCollect) {
        auto expressionName = expression->getUniqueName();
        auto dataPos = mapperContext.getDataPos(expressionName);
        auto isFlat = schema.getGroup(expressionName)->getIsFlat();
        valueVectorsToCollectInfo.emplace_back(dataPos, isFlat);
    }
    auto paramsString = string();
    for (auto& expression : expressionsToCollect) {
        paramsString += expression->getUniqueName() + ", ";
    }
    auto sharedState = make_shared<FTableSharedState>();
    return make_unique<ResultCollector>(
        valueVectorsToCollectInfo, sharedState, move(prevOperator), getOperatorID(), paramsString);
}

unique_ptr<PhysicalOperator> PlanMapper::createHashAggregate(
    vector<unique_ptr<AggregateFunction>> aggregateFunctions, vector<DataPos> inputAggVectorsPos,
    vector<DataPos> outputAggVectorsPos, vector<DataType> outputAggVectorsDataType,
    const expression_vector& groupByExpressions, Schema* schema,
    unique_ptr<PhysicalOperator> prevOperator, MapperContext& mapperContextBeforeAggregate,
    MapperContext& mapperContext, const string& paramsString) {
    vector<DataPos> inputGroupByHashKeyVectorsPos;
    vector<DataPos> inputGroupByNonHashKeyVectorsPos;
    vector<bool> isInputGroupByHashKeyVectorFlat;
    vector<DataPos> outputGroupByKeyVectorsPos;
    vector<DataType> outputGroupByKeyVectorsDataTypeId;
    expression_vector groupByHashExpressions;
    expression_vector groupByNonHashExpressions;
    unordered_set<string> HashPrimaryKeysNodeId;
    for (auto& expressionToGroupBy : groupByExpressions) {
        if (expressionToGroupBy->expressionType == PROPERTY) {
            auto& propertyExpression = (const PropertyExpression&)(*expressionToGroupBy);
            if (propertyExpression.isInternalID()) {
                groupByHashExpressions.push_back(expressionToGroupBy);
                HashPrimaryKeysNodeId.insert(propertyExpression.getChild(0)->getUniqueName());
            } else if (HashPrimaryKeysNodeId.contains(
                           propertyExpression.getChild(0)->getUniqueName())) {
                groupByNonHashExpressions.push_back(expressionToGroupBy);
            } else {
                groupByHashExpressions.push_back(expressionToGroupBy);
            }
        } else {
            groupByHashExpressions.push_back(expressionToGroupBy);
        }
    }
    appendGroupByExpressions(groupByHashExpressions, inputGroupByHashKeyVectorsPos,
        outputGroupByKeyVectorsPos, outputGroupByKeyVectorsDataTypeId, mapperContextBeforeAggregate,
        mapperContext, schema, isInputGroupByHashKeyVectorFlat);
    appendGroupByExpressions(groupByNonHashExpressions, inputGroupByNonHashKeyVectorsPos,
        outputGroupByKeyVectorsPos, outputGroupByKeyVectorsDataTypeId, mapperContextBeforeAggregate,
        mapperContext, schema, isInputGroupByHashKeyVectorFlat);
    auto sharedState = make_shared<HashAggregateSharedState>(aggregateFunctions);
    auto aggregate = make_unique<HashAggregate>(sharedState, inputGroupByHashKeyVectorsPos,
        inputGroupByNonHashKeyVectorsPos, isInputGroupByHashKeyVectorFlat, move(inputAggVectorsPos),
        move(aggregateFunctions), move(prevOperator), getOperatorID(), paramsString);
    auto aggregateScan = make_unique<HashAggregateScan>(sharedState,
        mapperContext.getResultSetDescriptor()->copy(), outputGroupByKeyVectorsPos,
        outputGroupByKeyVectorsDataTypeId, move(outputAggVectorsPos),
        move(outputAggVectorsDataType), move(aggregate), getOperatorID(), paramsString);
    return aggregateScan;
}

void PlanMapper::appendGroupByExpressions(const expression_vector& groupByExpressions,
    vector<DataPos>& inputGroupByHashKeyVectorsPos, vector<DataPos>& outputGroupByKeyVectorsPos,
    vector<DataType>& outputGroupByKeyVectorsDataTypes, MapperContext& mapperContextBeforeAggregate,
    MapperContext& mapperContext, Schema* schema, vector<bool>& isInputGroupByHashKeyVectorFlat) {
    for (auto& expression : groupByExpressions) {
        if (schema->getGroup(expression->getUniqueName())->getIsFlat()) {
            inputGroupByHashKeyVectorsPos.push_back(
                mapperContextBeforeAggregate.getDataPos(expression->getUniqueName()));
            outputGroupByKeyVectorsPos.push_back(
                mapperContext.getDataPos(expression->getUniqueName()));
            outputGroupByKeyVectorsDataTypes.push_back(expression->dataType);
            mapperContext.addComputedExpressions(expression->getUniqueName());
            isInputGroupByHashKeyVectorFlat.push_back(true);
        }
    }

    for (auto& expression : groupByExpressions) {
        if (!schema->getGroup(expression->getUniqueName())->getIsFlat()) {
            inputGroupByHashKeyVectorsPos.push_back(
                mapperContextBeforeAggregate.getDataPos(expression->getUniqueName()));
            outputGroupByKeyVectorsPos.push_back(
                mapperContext.getDataPos(expression->getUniqueName()));
            outputGroupByKeyVectorsDataTypes.push_back(expression->dataType);
            mapperContext.addComputedExpressions(expression->getUniqueName());
            isInputGroupByHashKeyVectorFlat.push_back(false);
        }
    }
}

} // namespace processor
} // namespace graphflow
