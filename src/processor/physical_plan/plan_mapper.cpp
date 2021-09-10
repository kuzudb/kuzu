#include "src/processor/include/physical_plan/plan_mapper.h"

#include <set>

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

unique_ptr<PhysicalPlan> PlanMapper::mapToPhysical(
    unique_ptr<LogicalPlan> logicalPlan, ExecutionContext& context) {
    auto info = PhysicalOperatorsInfo(*logicalPlan->schema);
    auto prevOperator = mapLogicalOperatorToPhysical(logicalPlan->lastOperator, info, context);
    auto resultCollector = make_unique<ResultCollector>(move(prevOperator), RESULT_COLLECTOR,
        context, physicalOperatorID++, !logicalPlan->isCountOnly);
    return make_unique<PhysicalPlan>(move(resultCollector));
}

pair<ResultSet*, PhysicalOperatorsInfo*> PlanMapper::enterSubquery(
    ResultSet* newOuterQueryResultSet, PhysicalOperatorsInfo* newPhysicalOperatorsInfo) {
    auto prevOuterQueryResultSet = outerQueryResultSet;
    auto prevPhysicalOperatorInfo = outerPhysicalOperatorsInfo;
    outerQueryResultSet = newOuterQueryResultSet;
    outerPhysicalOperatorsInfo = newPhysicalOperatorsInfo;
    return make_pair(prevOuterQueryResultSet, prevPhysicalOperatorInfo);
}

void PlanMapper::exitSubquery(
    ResultSet* prevOuterQueryResultSet, PhysicalOperatorsInfo* prevPhysicalOperatorsInfo) {
    outerQueryResultSet = prevOuterQueryResultSet;
    outerPhysicalOperatorsInfo = prevPhysicalOperatorsInfo;
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalOperatorToPhysical(
    const shared_ptr<LogicalOperator>& logicalOperator, PhysicalOperatorsInfo& info,
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
    default:
        assert(false);
    }
    // This map is populated regardless of whether PROFILE or EXPLAIN is enabled.
    physicalIDToLogicalOperatorMap.insert({physicalOperator->id, logicalOperator});
    return physicalOperator;
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanNodeIDToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context) {
    auto& scan = (const LogicalScanNodeID&)*logicalOperator;
    auto morsel = make_shared<MorselsDesc>(scan.label, graph.getNumNodes(scan.label));
    auto [outDataChunkPos, outValueVectorPos] = info.getDataChunkAndValueVectorPos(scan.nodeID);
    if (scan.prevOperator) {
        return make_unique<ScanNodeID>(info.getDataChunkSize(outDataChunkPos), outDataChunkPos,
            outValueVectorPos, morsel,
            mapLogicalOperatorToPhysical(scan.prevOperator, info, context), context,
            physicalOperatorID++);
    }
    return make_unique<ScanNodeID>(info.getTotalNumDataChunks(),
        info.getDataChunkSize(outDataChunkPos), outDataChunkPos, outValueVectorPos, morsel, context,
        physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSelectScanToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context) {
    auto& selectScan = (const LogicalSelectScan&)*logicalOperator;
    vector<pair<uint32_t, uint32_t>> inDataChunkAndValueVectorsPos;
    uint32_t outDataChunkPos =
        info.getDataChunkAndValueVectorPos(*selectScan.getVariablesToSelect().begin()).first;
    vector<uint32_t> outValueVectorPos;
    for (auto& variable : selectScan.getVariablesToSelect()) {
        inDataChunkAndValueVectorsPos.push_back(
            outerPhysicalOperatorsInfo->getDataChunkAndValueVectorPos(variable));
        // all variables should be appended to the same datachunk
        assert(outDataChunkPos == info.getDataChunkAndValueVectorPos(variable).first);
        outValueVectorPos.push_back(info.getDataChunkAndValueVectorPos(variable).second);
    }
    return make_unique<SelectScan>(info.getTotalNumDataChunks(), outerQueryResultSet,
        move(inDataChunkAndValueVectorsPos), info.getDataChunkSize(outDataChunkPos),
        outDataChunkPos, move(outValueVectorPos), context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalExtendToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context) {
    auto& extend = (const LogicalExtend&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->prevOperator, info, context);
    auto [inDataChunkPos, inValueVectorPos] =
        info.getDataChunkAndValueVectorPos(extend.boundNodeID);
    auto [outDataChunkPos, outValueVectorPos] =
        info.getDataChunkAndValueVectorPos(extend.nbrNodeID);
    auto& relsStore = graph.getRelsStore();
    if (extend.isColumn) {
        assert(inDataChunkPos == outDataChunkPos);
        assert(extend.lowerBound == extend.upperBound && extend.lowerBound == 1);
        return make_unique<AdjColumnExtend>(inDataChunkPos, inValueVectorPos, outValueVectorPos,
            relsStore.getAdjColumn(extend.direction, extend.boundNodeLabel, extend.relLabel),
            move(prevOperator), context, physicalOperatorID++);
    } else {
        auto adjLists =
            relsStore.getAdjLists(extend.direction, extend.boundNodeLabel, extend.relLabel);
        if (extend.lowerBound == 1 && extend.lowerBound == extend.upperBound) {
            return make_unique<AdjListExtend>(inDataChunkPos, inValueVectorPos,
                info.getDataChunkSize(outDataChunkPos), outDataChunkPos, outValueVectorPos,
                adjLists, move(prevOperator), context, physicalOperatorID++);
        } else {
            return make_unique<FrontierExtend>(inDataChunkPos, inValueVectorPos,
                info.getDataChunkSize(outDataChunkPos), outDataChunkPos, outValueVectorPos,
                adjLists, extend.nbrNodeLabel, extend.lowerBound, extend.upperBound,
                move(prevOperator), context, physicalOperatorID++);
        }
    }
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalFlattenToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context) {
    auto& flatten = (const LogicalFlatten&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->prevOperator, info, context);
    auto dataChunkPos = info.getDataChunkPos(flatten.variable);
    return make_unique<Flatten>(dataChunkPos, move(prevOperator), context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalFilterToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context) {
    auto& logicalFilter = (const LogicalFilter&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->prevOperator, info, context);
    auto dataChunkToSelectPos = logicalFilter.groupPosToSelect;
    auto physicalRootExpr = expressionMapper.mapToPhysical(
        *logicalFilter.expression, info, prevOperator->getResultSet().get(), context);
    return make_unique<Filter>(move(physicalRootExpr), dataChunkToSelectPos, move(prevOperator),
        context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalIntersectToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context) {
    auto& logicalIntersect = (const LogicalIntersect&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->prevOperator, info, context);
    auto [leftDataChunkPos, leftValueVectorPos] =
        info.getDataChunkAndValueVectorPos(logicalIntersect.getLeftNodeID());
    auto [rightDataChunkPos, rightValueVectorPos] =
        info.getDataChunkAndValueVectorPos(logicalIntersect.getRightNodeID());
    return make_unique<Intersect>(leftDataChunkPos, leftValueVectorPos, rightDataChunkPos,
        rightValueVectorPos, move(prevOperator), context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalProjectionToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context) {
    auto& logicalProjection = (const LogicalProjection&)*logicalOperator;
    auto oldInfo = PhysicalOperatorsInfo(*logicalProjection.schemaBeforeProjection);
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->prevOperator, oldInfo, context);
    vector<unique_ptr<ExpressionEvaluator>> expressionEvaluators;
    vector<pair<uint32_t, uint32_t>> expressionsOutputPos;
    for (auto& expression : logicalProjection.expressionsToProject) {
        expressionEvaluators.push_back(expressionMapper.mapToPhysical(
            *expression, oldInfo, prevOperator->getResultSet().get(), context));
        expressionsOutputPos.push_back(
            info.getDataChunkAndValueVectorPos(expression->getInternalName()));
    }
    vector<uint32_t> outDataChunksSize;
    for (auto i = 0u; i < info.getTotalNumDataChunks(); ++i) {
        outDataChunksSize.push_back(info.getDataChunkSize(i));
    }
    return make_unique<Projection>(info.getTotalNumDataChunks(), move(outDataChunksSize),
        move(expressionEvaluators), move(expressionsOutputPos), logicalProjection.discardedGroupPos,
        move(prevOperator), context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanNodePropertyToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context) {
    auto& scanProperty = (const LogicalScanNodeProperty&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->prevOperator, info, context);
    auto [inDataChunkPos, inValueVectorPos] =
        info.getDataChunkAndValueVectorPos(scanProperty.nodeID);
    auto [outDataChunkPos, outValueVectorPos] =
        info.getDataChunkAndValueVectorPos(scanProperty.propertyVariableName);
    auto& nodeStore = graph.getNodesStore();
    assert(inDataChunkPos == outDataChunkPos);
    if (scanProperty.isUnstructuredProperty) {
        auto lists = nodeStore.getNodeUnstrPropertyLists(scanProperty.nodeLabel);
        return make_unique<ScanUnstructuredProperty>(inDataChunkPos, inValueVectorPos,
            outValueVectorPos, scanProperty.propertyKey, lists, move(prevOperator), context,
            physicalOperatorID++);
    }
    auto column = nodeStore.getNodePropertyColumn(scanProperty.nodeLabel, scanProperty.propertyKey);
    return make_unique<ScanStructuredProperty>(inDataChunkPos, inValueVectorPos, outValueVectorPos,
        column, move(prevOperator), context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanRelPropertyToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context) {
    auto& scanProperty = (const LogicalScanRelProperty&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->prevOperator, info, context);
    auto [inDataChunkPos, inValueVectorPos] =
        info.getDataChunkAndValueVectorPos(scanProperty.boundNodeID);
    auto [outDataChunkPos, outValueVectorPos] =
        info.getDataChunkAndValueVectorPos(scanProperty.propertyVariableName);
    auto& relStore = graph.getRelsStore();
    if (scanProperty.isColumn) {
        assert(inDataChunkPos == outDataChunkPos);
        auto column = relStore.getRelPropertyColumn(
            scanProperty.relLabel, scanProperty.boundNodeLabel, scanProperty.propertyKey);
        return make_unique<ScanStructuredProperty>(inDataChunkPos, inValueVectorPos,
            outValueVectorPos, column, move(prevOperator), context, physicalOperatorID++);
    }
    auto lists = relStore.getRelPropertyLists(scanProperty.direction, scanProperty.boundNodeLabel,
        scanProperty.relLabel, scanProperty.propertyKey);
    return make_unique<ReadRelPropertyList>(inDataChunkPos, inValueVectorPos, outDataChunkPos,
        outValueVectorPos, lists, move(prevOperator), context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalHashJoinToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context) {
    auto& hashJoin = (const LogicalHashJoin&)*logicalOperator;
    // create hashJoin build
    auto buildSideInfo = PhysicalOperatorsInfo(*hashJoin.buildSideSchema);
    auto buildSidePrevOperator =
        mapLogicalOperatorToPhysical(hashJoin.buildSidePrevOperator, buildSideInfo, context);
    auto [buildSideKeyDataChunkPos, buildSideKeyVectorPos] =
        buildSideInfo.getDataChunkAndValueVectorPos(hashJoin.joinNodeID);
    vector<bool> dataChunkPosToIsFlat;
    for (auto& buildSideGroup : hashJoin.buildSideSchema->groups) {
        dataChunkPosToIsFlat.push_back(buildSideGroup->isFlat);
    }
    auto buildDataChunksInfo = BuildDataChunksInfo(
        buildSideKeyDataChunkPos, buildSideKeyVectorPos, move(dataChunkPosToIsFlat));
    auto hashJoinBuild = make_unique<HashJoinBuild>(
        buildDataChunksInfo, move(buildSidePrevOperator), context, physicalOperatorID++);
    auto hashJoinSharedState =
        make_shared<HashJoinSharedState>(hashJoinBuild->numBytesForFixedTuplePart);
    hashJoinBuild->sharedState = hashJoinSharedState;

    // create hashJoin probe
    auto probeSidePrevOperator = mapLogicalOperatorToPhysical(hashJoin.prevOperator, info, context);
    auto [probeSideKeyDataChunkPos, probeSideKeyVectorPos] =
        info.getDataChunkAndValueVectorPos(hashJoin.joinNodeID);
    auto flatDataChunkSize = hashJoin.probeSideFlatGroupPos == UINT32_MAX ?
                                 0 :
                                 info.getDataChunkSize(hashJoin.probeSideFlatGroupPos);
    vector<uint32_t> unFlatDataChunksSize;
    for (auto& unFlatDataChunkPos : hashJoin.probeSideUnFlatGroupsPos) {
        unFlatDataChunksSize.push_back(info.getDataChunkSize(unFlatDataChunkPos));
    }
    auto probeDataChunksInfo = ProbeDataChunksInfo(probeSideKeyDataChunkPos, probeSideKeyVectorPos,
        hashJoin.probeSideFlatGroupPos, flatDataChunkSize, hashJoin.probeSideUnFlatGroupsPos,
        unFlatDataChunksSize);

    // for each dataChunk on build side, we maintain a map from value vector position to its output
    // position of probe side.
    vector<unordered_map<uint32_t, pair<uint32_t, uint32_t>>> buildSideValueVectorsOutputPos;
    for (auto i = 0u; i < hashJoin.buildSideSchema->groups.size(); ++i) {
        buildSideValueVectorsOutputPos.emplace_back(
            unordered_map<uint32_t, pair<uint32_t, uint32_t>>());
        auto& buildSideGroup = *hashJoin.buildSideSchema->groups[i];
        for (auto& variable : buildSideGroup.variables) {
            if (i == buildSideKeyDataChunkPos && variable == hashJoin.joinNodeID) {
                continue;
            }
            auto [buildSideDataChunkPos, buildSideValueVectorPos] =
                buildSideInfo.getDataChunkAndValueVectorPos(variable);
            assert(buildSideDataChunkPos == i);
            buildSideValueVectorsOutputPos[i].insert(
                {buildSideValueVectorPos, info.getDataChunkAndValueVectorPos(variable)});
        }
    }

    auto hashJoinProbe = make_unique<HashJoinProbe>(buildDataChunksInfo, probeDataChunksInfo,
        move(buildSideValueVectorsOutputPos), move(hashJoinBuild), move(probeSidePrevOperator),
        context, physicalOperatorID++);
    hashJoinProbe->sharedState = hashJoinSharedState;
    return hashJoinProbe;
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalLoadCSVToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context) {
    auto& logicalLoadCSV = (const LogicalLoadCSV&)*logicalOperator;
    assert(!logicalLoadCSV.csvColumnVariables.empty());
    vector<DataType> csvColumnDataTypes;
    uint32_t outDataChunkPos =
        info.getDataChunkPos(logicalLoadCSV.csvColumnVariables[0]->getInternalName());
    vector<uint32_t> outValueVectorsPos;
    for (auto& csvColumnVariable : logicalLoadCSV.csvColumnVariables) {
        csvColumnDataTypes.push_back(csvColumnVariable->dataType);
        auto [dataChunkPos, outValueVectorPos] =
            info.getDataChunkAndValueVectorPos(csvColumnVariable->getInternalName());
        assert(outDataChunkPos == dataChunkPos);
        outValueVectorsPos.push_back(outValueVectorPos);
    }
    return make_unique<LoadCSV>(logicalLoadCSV.path, logicalLoadCSV.tokenSeparator,
        csvColumnDataTypes, info.getTotalNumDataChunks(), info.getDataChunkSize(outDataChunkPos),
        outDataChunkPos, move(outValueVectorsPos), context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalMultiplicityReducerToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context) {
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->prevOperator, info, context);
    return make_unique<MultiplicityReducer>(move(prevOperator), context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSkipToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context) {
    auto& logicalSkip = (const LogicalSkip&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->prevOperator, info, context);
    auto dataChunkToSelectPos = logicalSkip.getGroupPosToSelect();
    vector<uint64_t> dataChunksToLimit;
    for (auto& groupPosToSkip : logicalSkip.getGroupsPosToSkip()) {
        dataChunksToLimit.push_back(groupPosToSkip);
    }
    return make_unique<Skip>(logicalSkip.getSkipNumber(), make_shared<atomic_uint64_t>(0),
        dataChunkToSelectPos, move(dataChunksToLimit), move(prevOperator), context,
        physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalLimitToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context) {
    auto& logicalLimit = (const LogicalLimit&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->prevOperator, info, context);
    auto dataChunkToSelectPos = logicalLimit.getGroupPosToSelect();
    vector<uint64_t> dataChunksToLimit;
    for (auto& groupPosToLimit : logicalLimit.getGroupsPosToLimit()) {
        dataChunksToLimit.push_back(groupPosToLimit);
    }
    return make_unique<Limit>(logicalLimit.getLimitNumber(), make_shared<atomic_uint64_t>(0),
        dataChunkToSelectPos, move(dataChunksToLimit), move(prevOperator), context,
        physicalOperatorID++);
}

} // namespace processor
} // namespace graphflow
