#include "src/processor/include/physical_plan/plan_mapper.h"

#include <set>

#include "src/expression_evaluator/include/aggregate_expression_evaluator.h"
#include "src/planner/include/logical_plan/operator/aggregate/logical_aggregate.h"
#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/filter/logical_filter.h"
#include "src/planner/include/logical_plan/operator/flatten/logical_flatten.h"
#include "src/planner/include/logical_plan/operator/hash_join/logical_hash_join.h"
#include "src/planner/include/logical_plan/operator/intersect/logical_intersect.h"
#include "src/planner/include/logical_plan/operator/limit/logical_limit.h"
#include "src/planner/include/logical_plan/operator/load_csv/logical_load_csv.h"
#include "src/planner/include/logical_plan/operator/projection/logical_projection.h"
#include "src/planner/include/logical_plan/operator/scan_node_id/logical_scan_node_id.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_node_property.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_rel_property.h"
#include "src/planner/include/logical_plan/operator/select_scan/logical_select_scan.h"
#include "src/planner/include/logical_plan/operator/skip/logical_skip.h"
#include "src/processor/include/physical_plan/expression_mapper.h"
#include "src/processor/include/physical_plan/operator/aggregate/aggregate.h"
#include "src/processor/include/physical_plan/operator/aggregate/aggregation_scan.h"
#include "src/processor/include/physical_plan/operator/filter/filter.h"
#include "src/processor/include/physical_plan/operator/flatten/flatten.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"
#include "src/processor/include/physical_plan/operator/intersect/intersect.h"
#include "src/processor/include/physical_plan/operator/limit/limit.h"
#include "src/processor/include/physical_plan/operator/load_csv/load_csv.h"
#include "src/processor/include/physical_plan/operator/multiplicity_reducer/multiplicity_reducer.h"
#include "src/processor/include/physical_plan/operator/projection/projection.h"
#include "src/processor/include/physical_plan/operator/read_list/adj_list_extend.h"
#include "src/processor/include/physical_plan/operator/read_list/frontier_extend.h"
#include "src/processor/include/physical_plan/operator/read_list/read_rel_property_list.h"
#include "src/processor/include/physical_plan/operator/scan_attribute/adj_column_extend.h"
#include "src/processor/include/physical_plan/operator/scan_attribute/scan_structured_property.h"
#include "src/processor/include/physical_plan/operator/scan_attribute/scan_unstructured_property.h"
#include "src/processor/include/physical_plan/operator/scan_node_id/scan_node_id.h"
#include "src/processor/include/physical_plan/operator/select_scan/select_scan.h"
#include "src/processor/include/physical_plan/operator/sink/result_collector.h"
#include "src/processor/include/physical_plan/operator/skip/skip.h"

using namespace graphflow::planner;

namespace graphflow {
namespace processor {

static shared_ptr<ResultSet> populateResultSet(const Schema& schema) {
    auto resultSet = make_shared<ResultSet>(schema.getNumGroups());
    for (auto i = 0u; i < schema.getNumGroups(); ++i) {
        resultSet->insert(i, make_shared<DataChunk>(schema.getGroup(i)->getNumVariables()));
    }
    return resultSet;
}

unique_ptr<PhysicalPlan> PlanMapper::mapToPhysical(
    unique_ptr<LogicalPlan> logicalPlan, ExecutionContext& context) {
    auto& schema = *logicalPlan->getSchema();
    auto info = PhysicalOperatorsInfo(schema);
    vector<DataPos> valueVectorsToCollectPos;
    for (auto& expression : schema.expressionsToCollect) {
        valueVectorsToCollectPos.push_back(info.getDataPos(expression->getInternalName()));
    }
    auto prevOperator = mapLogicalOperatorToPhysical(logicalPlan->lastOperator, info, context);
    auto resultCollector =
        make_unique<ResultCollector>(populateResultSet(schema), move(valueVectorsToCollectPos),
            move(prevOperator), RESULT_COLLECTOR, context, physicalOperatorID++);
    return make_unique<PhysicalPlan>(move(resultCollector));
}

const PhysicalOperatorsInfo* PlanMapper::enterSubquery(
    const PhysicalOperatorsInfo* newPhysicalOperatorsInfo) {
    auto prevPhysicalOperatorInfo = outerPhysicalOperatorsInfo;
    outerPhysicalOperatorsInfo = newPhysicalOperatorsInfo;
    return prevPhysicalOperatorInfo;
}

void PlanMapper::exitSubquery(const PhysicalOperatorsInfo* prevPhysicalOperatorsInfo) {
    outerPhysicalOperatorsInfo = prevPhysicalOperatorsInfo;
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalOperatorToPhysical(
    const shared_ptr<LogicalOperator>& logicalOperator, const PhysicalOperatorsInfo& info,
    ExecutionContext& context) {
    unique_ptr<PhysicalOperator> physicalOperator;
    auto operatorType = logicalOperator->getLogicalOperatorType();
    switch (operatorType) {
    case LOGICAL_SCAN_NODE_ID:
        physicalOperator = mapLogicalScanNodeIDToPhysical(logicalOperator.get(), info, context);
        break;
    case LOGICAL_SELECT_SCAN:
        physicalOperator = mapLogicalSelectScanToPhysical(logicalOperator.get(), info, context);
        break;
    case LOGICAL_EXTEND:
        physicalOperator = mapLogicalExtendToPhysical(logicalOperator.get(), info, context);
        break;
    case LOGICAL_FLATTEN:
        physicalOperator = mapLogicalFlattenToPhysical(logicalOperator.get(), info, context);
        break;
    case LOGICAL_FILTER:
        physicalOperator = mapLogicalFilterToPhysical(logicalOperator.get(), info, context);
        break;
    case LOGICAL_INTERSECT:
        physicalOperator = mapLogicalIntersectToPhysical(logicalOperator.get(), info, context);
        break;
    case LOGICAL_PROJECTION:
        physicalOperator = mapLogicalProjectionToPhysical(logicalOperator.get(), info, context);
        break;
    case LOGICAL_HASH_JOIN:
        physicalOperator = mapLogicalHashJoinToPhysical(logicalOperator.get(), info, context);
        break;
    case LOGICAL_SCAN_NODE_PROPERTY:
        physicalOperator =
            mapLogicalScanNodePropertyToPhysical(logicalOperator.get(), info, context);
        break;
    case LOGICAL_SCAN_REL_PROPERTY:
        physicalOperator =
            mapLogicalScanRelPropertyToPhysical(logicalOperator.get(), info, context);
        break;
    case LOGICAL_LOAD_CSV:
        physicalOperator = mapLogicalLoadCSVToPhysical(logicalOperator.get(), info, context);
        break;
    case LOGICAL_MULTIPLICITY_REDUCER:
        physicalOperator =
            mapLogicalMultiplicityReducerToPhysical(logicalOperator.get(), info, context);
        break;
    case LOGICAL_SKIP:
        physicalOperator = mapLogicalSkipToPhysical(logicalOperator.get(), info, context);
        break;
    case LOGICAL_LIMIT:
        physicalOperator = mapLogicalLimitToPhysical(logicalOperator.get(), info, context);
        break;
    case LOGICAL_AGGREGATE:
        physicalOperator = mapLogicalAggregateToPhysical(logicalOperator.get(), info, context);
        break;
    default:
        assert(false);
    }
    // This map is populated regardless of whether PROFILE or EXPLAIN is enabled.
    physicalIDToLogicalOperatorMap.insert({physicalOperator->id, logicalOperator});
    return physicalOperator;
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanNodeIDToPhysical(
    LogicalOperator* logicalOperator, const PhysicalOperatorsInfo& info,
    ExecutionContext& context) {
    auto& logicalScan = (const LogicalScanNodeID&)*logicalOperator;
    auto morsel = make_shared<MorselsDesc>(logicalScan.label, graph.getNumNodes(logicalScan.label));
    auto dataPos = info.getDataPos(logicalScan.nodeID);
    if (logicalScan.prevOperator) {
        return make_unique<ScanNodeID>(dataPos, morsel,
            mapLogicalOperatorToPhysical(logicalScan.prevOperator, info, context), context,
            physicalOperatorID++);
    }
    return make_unique<ScanNodeID>(dataPos, morsel, context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSelectScanToPhysical(
    LogicalOperator* logicalOperator, const PhysicalOperatorsInfo& info,
    ExecutionContext& context) {
    auto& selectScan = (const LogicalSelectScan&)*logicalOperator;
    vector<DataPos> inDataPoses;
    uint32_t outDataChunkPos =
        info.getDataPos(*selectScan.getVariablesToSelect().begin()).dataChunkPos;
    vector<uint32_t> outValueVectorsPos;
    for (auto& variable : selectScan.getVariablesToSelect()) {
        inDataPoses.push_back(outerPhysicalOperatorsInfo->getDataPos(variable));
        // all variables should be appended to the same datachunk
        assert(outDataChunkPos == info.getDataPos(variable).dataChunkPos);
        outValueVectorsPos.push_back(info.getDataPos(variable).valueVectorPos);
    }
    return make_unique<SelectScan>(move(inDataPoses), outDataChunkPos, move(outValueVectorsPos),
        context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalExtendToPhysical(
    LogicalOperator* logicalOperator, const PhysicalOperatorsInfo& info,
    ExecutionContext& context) {
    auto& extend = (const LogicalExtend&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->prevOperator, info, context);
    auto inDataPos = info.getDataPos(extend.boundNodeID);
    auto outDataPos = info.getDataPos(extend.nbrNodeID);
    auto& relsStore = graph.getRelsStore();
    if (extend.isColumn) {
        assert(extend.lowerBound == extend.upperBound && extend.lowerBound == 1);
        return make_unique<AdjColumnExtend>(inDataPos, outDataPos,
            relsStore.getAdjColumn(extend.direction, extend.boundNodeLabel, extend.relLabel),
            move(prevOperator), context, physicalOperatorID++);
    } else {
        auto adjLists =
            relsStore.getAdjLists(extend.direction, extend.boundNodeLabel, extend.relLabel);
        if (extend.lowerBound == 1 && extend.lowerBound == extend.upperBound) {
            return make_unique<AdjListExtend>(
                inDataPos, outDataPos, adjLists, move(prevOperator), context, physicalOperatorID++);
        } else {
            return make_unique<FrontierExtend>(inDataPos, outDataPos, adjLists, extend.nbrNodeLabel,
                extend.lowerBound, extend.upperBound, move(prevOperator), context,
                physicalOperatorID++);
        }
    }
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalFlattenToPhysical(
    LogicalOperator* logicalOperator, const PhysicalOperatorsInfo& info,
    ExecutionContext& context) {
    auto& flatten = (const LogicalFlatten&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->prevOperator, info, context);
    return make_unique<Flatten>(info.getDataPos(flatten.variable).dataChunkPos, move(prevOperator),
        context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalFilterToPhysical(
    LogicalOperator* logicalOperator, const PhysicalOperatorsInfo& info,
    ExecutionContext& context) {
    auto& logicalFilter = (const LogicalFilter&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->prevOperator, info, context);
    auto dataChunkToSelectPos = logicalFilter.groupPosToSelect;
    auto physicalRootExpr =
        expressionMapper.mapToPhysical(*logicalFilter.expression, info, context);
    return make_unique<Filter>(move(physicalRootExpr), dataChunkToSelectPos, move(prevOperator),
        context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalIntersectToPhysical(
    LogicalOperator* logicalOperator, const PhysicalOperatorsInfo& info,
    ExecutionContext& context) {
    auto& logicalIntersect = (const LogicalIntersect&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->prevOperator, info, context);
    auto leftDataPos = info.getDataPos(logicalIntersect.getLeftNodeID());
    auto rightDataPos = info.getDataPos(logicalIntersect.getRightNodeID());
    return make_unique<Intersect>(
        leftDataPos, rightDataPos, move(prevOperator), context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalProjectionToPhysical(
    LogicalOperator* logicalOperator, const PhysicalOperatorsInfo& info,
    ExecutionContext& context) {
    auto& logicalProjection = (const LogicalProjection&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->prevOperator, info, context);
    vector<unique_ptr<ExpressionEvaluator>> expressionEvaluators;
    vector<DataPos> expressionsOutputPos;
    for (auto& expression : logicalProjection.expressionsToProject) {
        expressionEvaluators.push_back(expressionMapper.mapToPhysical(*expression, info, context));
        expressionsOutputPos.push_back(info.getDataPos(expression->getInternalName()));
    }
    return make_unique<Projection>(move(expressionEvaluators), move(expressionsOutputPos),
        logicalProjection.discardedGroupsPos, move(prevOperator), context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanNodePropertyToPhysical(
    LogicalOperator* logicalOperator, const PhysicalOperatorsInfo& info,
    ExecutionContext& context) {
    auto& scanProperty = (const LogicalScanNodeProperty&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->prevOperator, info, context);
    auto inDataPos = info.getDataPos(scanProperty.nodeID);
    auto outDataPos = info.getDataPos(scanProperty.propertyVariableName);
    auto& nodeStore = graph.getNodesStore();
    if (scanProperty.isUnstructuredProperty) {
        auto lists = nodeStore.getNodeUnstrPropertyLists(scanProperty.nodeLabel);
        return make_unique<ScanUnstructuredProperty>(inDataPos, outDataPos,
            scanProperty.propertyKey, lists, move(prevOperator), context, physicalOperatorID++);
    }
    auto column = nodeStore.getNodePropertyColumn(scanProperty.nodeLabel, scanProperty.propertyKey);
    return make_unique<ScanStructuredProperty>(
        inDataPos, outDataPos, column, move(prevOperator), context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanRelPropertyToPhysical(
    LogicalOperator* logicalOperator, const PhysicalOperatorsInfo& info,
    ExecutionContext& context) {
    auto& scanProperty = (const LogicalScanRelProperty&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->prevOperator, info, context);
    auto inDataPos = info.getDataPos(scanProperty.boundNodeID);
    auto outDataPos = info.getDataPos(scanProperty.propertyVariableName);
    auto& relStore = graph.getRelsStore();
    if (scanProperty.isColumn) {
        auto column = relStore.getRelPropertyColumn(
            scanProperty.relLabel, scanProperty.boundNodeLabel, scanProperty.propertyKey);
        return make_unique<ScanStructuredProperty>(
            inDataPos, outDataPos, column, move(prevOperator), context, physicalOperatorID++);
    }
    auto lists = relStore.getRelPropertyLists(scanProperty.direction, scanProperty.boundNodeLabel,
        scanProperty.relLabel, scanProperty.propertyKey);
    return make_unique<ReadRelPropertyList>(
        inDataPos, outDataPos, lists, move(prevOperator), context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalHashJoinToPhysical(
    LogicalOperator* logicalOperator, const PhysicalOperatorsInfo& info,
    ExecutionContext& context) {
    auto& hashJoin = (const LogicalHashJoin&)*logicalOperator;
    // create hashJoin build
    auto buildSideInfo = PhysicalOperatorsInfo(*hashJoin.buildSideSchema);
    auto buildSidePrevOperator =
        mapLogicalOperatorToPhysical(hashJoin.buildSidePrevOperator, buildSideInfo, context);
    vector<bool> dataChunkPosToIsFlat;
    for (auto& buildSideGroup : hashJoin.buildSideSchema->groups) {
        dataChunkPosToIsFlat.push_back(buildSideGroup->isFlat);
    }
    auto buildDataChunksInfo = BuildDataChunksInfo(
        buildSideInfo.getDataPos(hashJoin.joinNodeID), move(dataChunkPosToIsFlat));
    auto hashJoinBuild = make_unique<HashJoinBuild>(buildDataChunksInfo,
        populateResultSet(*hashJoin.buildSideSchema), move(buildSidePrevOperator), context,
        physicalOperatorID++);
    auto hashJoinSharedState = make_shared<HashJoinSharedState>();
    hashJoinBuild->sharedState = hashJoinSharedState;

    // create hashJoin probe
    auto probeSidePrevOperator = mapLogicalOperatorToPhysical(hashJoin.prevOperator, info, context);
    auto probeDataChunksInfo = ProbeDataChunksInfo(info.getDataPos(hashJoin.joinNodeID),
        hashJoin.probeSideFlatGroupPos, hashJoin.probeSideUnFlatGroupsPos);
    // for each dataChunk on build side, we maintain a map from value vector position to its output
    // position of probe side.
    vector<unordered_map<uint32_t, DataPos>> buildSideValueVectorsOutputPos;
    for (auto i = 0u; i < hashJoin.buildSideSchema->groups.size(); ++i) {
        buildSideValueVectorsOutputPos.emplace_back(unordered_map<uint32_t, DataPos>());
        auto& buildSideGroup = *hashJoin.buildSideSchema->groups[i];
        for (auto& variable : buildSideGroup.variables) {
            if (i == buildDataChunksInfo.keyDataPos.dataChunkPos &&
                variable == hashJoin.joinNodeID) {
                continue;
            }
            auto [buildSideDataChunkPos, buildSideValueVectorPos] =
                buildSideInfo.getDataPos(variable);
            assert(buildSideDataChunkPos == i);
            buildSideValueVectorsOutputPos[i].insert(
                {buildSideValueVectorPos, info.getDataPos(variable)});
        }
    }

    auto hashJoinProbe = make_unique<HashJoinProbe>(buildDataChunksInfo, probeDataChunksInfo,
        move(buildSideValueVectorsOutputPos), move(hashJoinBuild), move(probeSidePrevOperator),
        context, physicalOperatorID++);
    hashJoinProbe->sharedState = hashJoinSharedState;
    return hashJoinProbe;
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalLoadCSVToPhysical(
    LogicalOperator* logicalOperator, const PhysicalOperatorsInfo& info,
    ExecutionContext& context) {
    auto& logicalLoadCSV = (const LogicalLoadCSV&)*logicalOperator;
    assert(!logicalLoadCSV.csvColumnVariables.empty());
    vector<DataType> csvColumnDataTypes;
    uint32_t outDataChunkPos =
        info.getDataPos(logicalLoadCSV.csvColumnVariables[0]->getInternalName()).dataChunkPos;
    vector<uint32_t> outValueVectorsPos;
    for (auto& csvColumnVariable : logicalLoadCSV.csvColumnVariables) {
        csvColumnDataTypes.push_back(csvColumnVariable->dataType);
        auto [dataChunkPos, outValueVectorPos] =
            info.getDataPos(csvColumnVariable->getInternalName());
        assert(outDataChunkPos == dataChunkPos);
        outValueVectorsPos.push_back(outValueVectorPos);
    }
    return make_unique<LoadCSV>(logicalLoadCSV.path, logicalLoadCSV.tokenSeparator,
        csvColumnDataTypes, outDataChunkPos, move(outValueVectorsPos), context,
        physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalMultiplicityReducerToPhysical(
    LogicalOperator* logicalOperator, const PhysicalOperatorsInfo& info,
    ExecutionContext& context) {
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->prevOperator, info, context);
    return make_unique<MultiplicityReducer>(move(prevOperator), context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSkipToPhysical(LogicalOperator* logicalOperator,
    const PhysicalOperatorsInfo& info, ExecutionContext& context) {
    auto& logicalSkip = (const LogicalSkip&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->prevOperator, info, context);
    auto dataChunkToSelectPos = logicalSkip.getGroupPosToSelect();
    return make_unique<Skip>(logicalSkip.getSkipNumber(), make_shared<atomic_uint64_t>(0),
        dataChunkToSelectPos, logicalSkip.getGroupsPosToSkip(), move(prevOperator), context,
        physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalLimitToPhysical(LogicalOperator* logicalOperator,
    const PhysicalOperatorsInfo& info, ExecutionContext& context) {
    auto& logicalLimit = (const LogicalLimit&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->prevOperator, info, context);
    auto dataChunkToSelectPos = logicalLimit.getGroupPosToSelect();
    return make_unique<Limit>(logicalLimit.getLimitNumber(), make_shared<atomic_uint64_t>(0),
        dataChunkToSelectPos, logicalLimit.getGroupsPosToLimit(), move(prevOperator), context,
        physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalAggregateToPhysical(
    LogicalOperator* logicalOperator, const PhysicalOperatorsInfo& info,
    ExecutionContext& context) {
    auto& logicalAggregate = (const LogicalAggregate&)*logicalOperator;
    auto infoBeforeAggregate = PhysicalOperatorsInfo(*logicalAggregate.getSchemaBeforeAggregate());
    auto resultSetBeforeAggregate = populateResultSet(*logicalAggregate.getSchemaBeforeAggregate());
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->prevOperator, infoBeforeAggregate, context);
    // Map aggregate expressions.
    vector<unique_ptr<AggregateExpressionEvaluator>> expressionEvaluators;
    vector<DataPos> expressionsOutputPos;
    for (auto& expression : logicalAggregate.getExpressionsToAggregate()) {
        expressionEvaluators.push_back(
            static_unique_pointer_cast<ExpressionEvaluator, AggregateExpressionEvaluator>(
                expressionMapper.mapToPhysical(*expression, infoBeforeAggregate, context)));
        expressionsOutputPos.push_back(info.getDataPos(expression->getInternalName()));
    }
    assert(!expressionsOutputPos.empty());
    auto outDataChunkPos = expressionsOutputPos[0].dataChunkPos;
    for (auto& dataPos : expressionsOutputPos) {
        // All expressions have the same out dataChunkPos;
        assert(dataPos.dataChunkPos == outDataChunkPos);
    }
    // Create the shared state.
    vector<unique_ptr<AggregationState>> aggregationStates;
    vector<DataType> aggregationDataTypes;
    for (auto& expressionEvaluator : expressionEvaluators) {
        aggregationStates.push_back(expressionEvaluator->getFunction()->initialize());
        aggregationDataTypes.push_back(expressionEvaluator->dataType);
    }
    auto sharedState =
        make_shared<AggregationSharedState>(move(aggregationStates), aggregationDataTypes);

    auto aggregate = make_unique<Aggregate>(move(resultSetBeforeAggregate), move(prevOperator),
        context, physicalOperatorID++, sharedState, move(expressionEvaluators));
    auto aggregationScan = make_unique<AggregationScan>(
        expressionsOutputPos, move(aggregate), sharedState, context, physicalOperatorID++);
    return aggregationScan;
}

} // namespace processor
} // namespace graphflow
