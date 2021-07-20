#include "src/processor/include/physical_plan/plan_mapper.h"

#include <set>

#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/filter/logical_filter.h"
#include "src/planner/include/logical_plan/operator/flatten/logical_flatten.h"
#include "src/planner/include/logical_plan/operator/hash_join/logical_hash_join.h"
#include "src/planner/include/logical_plan/operator/load_csv/logical_load_csv.h"
#include "src/planner/include/logical_plan/operator/projection/logical_projection.h"
#include "src/planner/include/logical_plan/operator/scan_node_id/logical_scan_node_id.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_node_property.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_rel_property.h"
#include "src/processor/include/physical_plan/expression_mapper.h"
#include "src/processor/include/physical_plan/operator/filter/filter.h"
#include "src/processor/include/physical_plan/operator/flatten/flatten.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"
#include "src/processor/include/physical_plan/operator/load_csv/load_csv.h"
#include "src/processor/include/physical_plan/operator/projection/projection.h"
#include "src/processor/include/physical_plan/operator/read_list/adj_list_extend.h"
#include "src/processor/include/physical_plan/operator/read_list/frontier_extend.h"
#include "src/processor/include/physical_plan/operator/read_list/read_rel_property_list.h"
#include "src/processor/include/physical_plan/operator/scan_attribute/adj_column_extend.h"
#include "src/processor/include/physical_plan/operator/scan_attribute/scan_structured_property.h"
#include "src/processor/include/physical_plan/operator/scan_attribute/scan_unstructured_property.h"
#include "src/processor/include/physical_plan/operator/scan_node_id/scan_node_id.h"
#include "src/processor/include/physical_plan/operator/sink/result_collector.h"

using namespace graphflow::planner;

namespace graphflow {
namespace processor {

unique_ptr<PhysicalPlan> PlanMapper::mapToPhysical(
    unique_ptr<LogicalPlan> logicalPlan, ExecutionContext& context) {
    auto info = PhysicalOperatorsInfo(*logicalPlan->schema);
    auto prevOperator = mapLogicalOperatorToPhysical(logicalPlan->lastOperator, info, context);
    auto resultCollector = make_unique<ResultCollector>(move(prevOperator), RESULT_COLLECTOR,
        context, physicalOperatorID++,
        logicalPlan->lastOperator->getLogicalOperatorType() == LOGICAL_PROJECTION);
    return make_unique<PhysicalPlan>(move(resultCollector));
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
    case LOGICAL_EXTEND:
        physicalOperator = mapLogicalExtendToPhysical(logicalOperator.get(), info, context);
        break;
    case LOGICAL_FLATTEN:
        physicalOperator = mapLogicalFlattenToPhysical(logicalOperator.get(), info, context);
        break;
    case LOGICAL_FILTER:
        physicalOperator = mapLogicalFilterToPhysical(logicalOperator.get(), info, context);
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
    default:
        GF_ASSERT(false);
    }
    // This map is populated regardless of whether PROFILE or EXPLAIN is enabled.
    physicalIDToLogicalOperatorMap.insert({physicalOperator->id, logicalOperator});
    return physicalOperator;
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanNodeIDToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context) {
    auto& scan = (const LogicalScanNodeID&)*logicalOperator;
    unique_ptr<PhysicalOperator> prevOperator;
    if (scan.prevOperator) {
        prevOperator = mapLogicalOperatorToPhysical(scan.prevOperator, info, context);
    }
    auto morsel = make_shared<MorselsDesc>(scan.label, graph.getNumNodes(scan.label));
    return prevOperator ? make_unique<ScanNodeID<true>>(
                              morsel, move(prevOperator), context, physicalOperatorID++) :
                          make_unique<ScanNodeID<true>>(morsel, context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalExtendToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context) {
    auto& extend = (const LogicalExtend&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->prevOperator, info, context);
    auto dataChunkPos = info.getDataChunkPos(extend.boundNodeID);
    auto valueVectorPos = info.getValueVectorPos(extend.boundNodeID);
    auto& relsStore = graph.getRelsStore();
    if (extend.isColumn) {
        GF_ASSERT(extend.lowerBound == extend.upperBound && extend.lowerBound == 1);
        return make_unique<AdjColumnExtend>(dataChunkPos, valueVectorPos,
            relsStore.getAdjColumn(extend.direction, extend.boundNodeLabel, extend.relLabel),
            move(prevOperator), context, physicalOperatorID++);
    } else {
        auto adjLists =
            relsStore.getAdjLists(extend.direction, extend.boundNodeLabel, extend.relLabel);
        if (extend.lowerBound == 1 && extend.lowerBound == extend.upperBound) {
            return make_unique<AdjListExtend<true>>(dataChunkPos, valueVectorPos, adjLists,
                move(prevOperator), context, physicalOperatorID++);
        } else {
            return make_unique<FrontierExtend<true>>(dataChunkPos, valueVectorPos, adjLists,
                extend.lowerBound, extend.upperBound, move(prevOperator), context,
                physicalOperatorID++);
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
    auto dataChunkToSelectPos = info.getDataChunkPos(logicalFilter.variableToSelect);
    auto physicalRootExpr = ExpressionMapper::mapToPhysical(
        *context.memoryManager, *logicalFilter.expression, info, *prevOperator->getResultSet());
    if (prevOperator->operatorType == FLATTEN) {
        return make_unique<Filter<true /* isAfterFlatten */>>(move(physicalRootExpr),
            dataChunkToSelectPos, move(prevOperator), context, physicalOperatorID++);
    } else {
        return make_unique<Filter<false /* isAfterFlatten */>>(move(physicalRootExpr),
            dataChunkToSelectPos, move(prevOperator), context, physicalOperatorID++);
    }
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalProjectionToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& physicalOperatorInfo,
    ExecutionContext& context) {
    auto& logicalProjection = (const LogicalProjection&)*logicalOperator;
    auto oldInfo = PhysicalOperatorsInfo(*logicalProjection.schemaBeforeProjection);
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->prevOperator, oldInfo, context);
    vector<unique_ptr<ExpressionEvaluator>> expressionEvaluators;
    for (auto& expression : logicalProjection.expressionsToProject) {
        expressionEvaluators.push_back(ExpressionMapper::mapToPhysical(
            *context.memoryManager, *expression, oldInfo, *prevOperator->getResultSet()));
    }
    // TODO: fix uint32 and uint64 mixed usage
    vector<uint64_t> exprResultInDataChunkPos;
    for (auto& pos : logicalProjection.exprResultOutGroupPos) {
        exprResultInDataChunkPos.push_back(pos);
    }
    vector<uint64_t> discardedDataChunkPos;
    for (auto& pos : logicalProjection.discardedGroupPos) {
        discardedDataChunkPos.push_back(pos);
    }
    return make_unique<Projection>(move(expressionEvaluators), exprResultInDataChunkPos,
        discardedDataChunkPos, move(prevOperator), context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanNodePropertyToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context) {
    auto& scanProperty = (const LogicalScanNodeProperty&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->prevOperator, info, context);
    auto dataChunkPos = info.getDataChunkPos(scanProperty.nodeID);
    auto valueVectorPos = info.getValueVectorPos(scanProperty.nodeID);
    if (scanProperty.isUnstructuredProperty) {
        auto lists = graph.getNodesStore().getNodeUnstrPropertyLists(scanProperty.nodeLabel);
        return make_unique<ScanUnstructuredProperty>(dataChunkPos, valueVectorPos,
            scanProperty.propertyKey, lists, move(prevOperator), context, physicalOperatorID++);
    }
    auto column = graph.getNodesStore().getNodePropertyColumn(
        scanProperty.nodeLabel, scanProperty.propertyKey);
    return make_unique<ScanStructuredProperty>(
        dataChunkPos, valueVectorPos, column, move(prevOperator), context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanRelPropertyToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context) {
    auto& scanProperty = (const LogicalScanRelProperty&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->prevOperator, info, context);
    auto inDataChunkPos = info.getDataChunkPos(scanProperty.boundNodeID);
    auto inValueVectorPos = info.getValueVectorPos(scanProperty.boundNodeID);
    auto outDataChunkPos = info.getDataChunkPos(scanProperty.nbrNodeID);
    if (scanProperty.isColumn) {
        auto column = graph.getRelsStore().getRelPropertyColumn(
            scanProperty.relLabel, scanProperty.boundNodeLabel, scanProperty.propertyKey);
        return make_unique<ScanStructuredProperty>(inDataChunkPos, inValueVectorPos, column,
            move(prevOperator), context, physicalOperatorID++);
    }
    auto lists = graph.getRelsStore().getRelPropertyLists(scanProperty.direction,
        scanProperty.boundNodeLabel, scanProperty.relLabel, scanProperty.propertyKey);
    return make_unique<ReadRelPropertyList>(inDataChunkPos, inValueVectorPos, outDataChunkPos,
        lists, move(prevOperator), context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalHashJoinToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context) {
    auto& hashJoin = (const LogicalHashJoin&)*logicalOperator;
    auto probeSidePrevOperator = mapLogicalOperatorToPhysical(hashJoin.prevOperator, info, context);
    auto buildSideInfo = PhysicalOperatorsInfo(*hashJoin.buildSideSchema);
    auto buildSidePrevOperator =
        mapLogicalOperatorToPhysical(hashJoin.buildSidePrevOperator, buildSideInfo, context);
    auto probeSideKeyDataChunkPos = info.getDataChunkPos(hashJoin.joinNodeID);
    auto probeSideKeyVectorPos = info.getValueVectorPos(hashJoin.joinNodeID);
    auto buildSideKeyDataChunkPos = buildSideInfo.getDataChunkPos(hashJoin.joinNodeID);
    auto buildSideKeyVectorPos = buildSideInfo.getValueVectorPos(hashJoin.joinNodeID);
    vector<bool> dataChunkPosToIsFlat;
    for (auto& buildSideGroup : hashJoin.buildSideSchema->groups) {
        dataChunkPosToIsFlat.push_back(buildSideGroup.isFlat);
    }
    auto hashJoinBuild = make_unique<HashJoinBuild>(buildSideKeyDataChunkPos, buildSideKeyVectorPos,
        dataChunkPosToIsFlat, move(buildSidePrevOperator), context, physicalOperatorID++);
    auto hashJoinSharedState =
        make_shared<HashJoinSharedState>(hashJoinBuild->numBytesForFixedTuplePart);
    hashJoinBuild->sharedState = hashJoinSharedState;
    auto hashJoinProbe =
        make_unique<HashJoinProbe<true>>(buildSideKeyDataChunkPos, buildSideKeyVectorPos,
            dataChunkPosToIsFlat, probeSideKeyDataChunkPos, probeSideKeyVectorPos,
            move(hashJoinBuild), move(probeSidePrevOperator), context, physicalOperatorID++);
    hashJoinProbe->sharedState = hashJoinSharedState;
    return hashJoinProbe;
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalLoadCSVToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& info, ExecutionContext& context) {
    auto& logicalLoadCSV = (const LogicalLoadCSV&)*logicalOperator;
    GF_ASSERT(!logicalLoadCSV.csvColumnVariables.empty());
    vector<DataType> csvColumnDataTypes;
    for (auto& csvColumnVariable : logicalLoadCSV.csvColumnVariables) {
        csvColumnDataTypes.push_back(csvColumnVariable->dataType);
    }
    return make_unique<LoadCSV<true>>(logicalLoadCSV.path, logicalLoadCSV.tokenSeparator,
        csvColumnDataTypes, context, physicalOperatorID++);
}

} // namespace processor
} // namespace graphflow
