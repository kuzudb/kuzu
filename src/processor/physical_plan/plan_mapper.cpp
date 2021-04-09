#include "src/processor/include/physical_plan/plan_mapper.h"

#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/filter/logical_filter.h"
#include "src/planner/include/logical_plan/operator/hash_join/logical_hash_join.h"
#include "src/planner/include/logical_plan/operator/scan_node_id/logical_scan_node_id.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_node_property.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_rel_property.h"
#include "src/processor/include/physical_plan/expression_mapper.h"
#include "src/processor/include/physical_plan/operator/filter/filter.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"
#include "src/processor/include/physical_plan/operator/read_list/extend/extend.h"
#include "src/processor/include/physical_plan/operator/read_list/extend/flatten_and_extend.h"
#include "src/processor/include/physical_plan/operator/read_list/read_rel_property_list.h"
#include "src/processor/include/physical_plan/operator/scan_column/adj_column_extend.h"
#include "src/processor/include/physical_plan/operator/scan_column/scan_node_property.h"
#include "src/processor/include/physical_plan/operator/scan_column/scan_rel_property.h"
#include "src/processor/include/physical_plan/operator/scan_node_id/scan_node_id.h"
#include "src/processor/include/physical_plan/operator/sink/result_collector.h"
#include "src/processor/include/physical_plan/operator/tuple/physical_operator_info.h"

using namespace graphflow::planner;

namespace graphflow {
namespace processor {

static unique_ptr<PhysicalOperator> mapLogicalOperatorToPhysical(
    const LogicalOperator& logicalOperator, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo);

static unique_ptr<PhysicalOperator> mapLogicalScanNodeIDToPhysical(
    const LogicalOperator& logicalOperator, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo);

static unique_ptr<PhysicalOperator> mapLogicalExtendToPhysical(
    const LogicalOperator& logicalOperator, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo);

static unique_ptr<PhysicalOperator> mapLogicalFilterToPhysical(
    const LogicalOperator& logicalOperator, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo);

static unique_ptr<PhysicalOperator> mapLogicalNodePropertyReaderToPhysical(
    const LogicalOperator& logicalOperator, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo);

static unique_ptr<PhysicalOperator> mapLogicalRelPropertyReaderToPhysical(
    const LogicalOperator& logicalOperator, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo);

static unique_ptr<PhysicalOperator> mapLogicalHashJoinToPhysical(
    const LogicalOperator& logicalOperator, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo);

unique_ptr<PhysicalPlan> PlanMapper::mapToPhysical(
    unique_ptr<LogicalPlan> logicalPlan, const Graph& graph) {
    auto physicalOperatorInfo = PhysicalOperatorsInfo();
    auto resultCollector = make_unique<ResultCollector>(
        mapLogicalOperatorToPhysical(logicalPlan->getLastOperator(), graph, physicalOperatorInfo));
    return make_unique<PhysicalPlan>(move(resultCollector));
}

unique_ptr<PhysicalOperator> mapLogicalOperatorToPhysical(const LogicalOperator& logicalOperator,
    const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto operatorType = logicalOperator.getLogicalOperatorType();
    switch (operatorType) {
    case LOGICAL_NODE_ID_SCAN:
        return mapLogicalScanNodeIDToPhysical(logicalOperator, graph, physicalOperatorInfo);
    case LOGICAL_EXTEND:
        return mapLogicalExtendToPhysical(logicalOperator, graph, physicalOperatorInfo);
    case LOGICAL_FILTER:
        // warning: assumes flattening is to be taken care of by the enumerator and not the mapper.
        //          we do not append any flattening currently.
        return mapLogicalFilterToPhysical(logicalOperator, graph, physicalOperatorInfo);
    case LOGICAL_HASH_JOIN:
        return mapLogicalHashJoinToPhysical(logicalOperator, graph, physicalOperatorInfo);
    case LOGICAL_SCAN_NODE_PROPERTY:
        return mapLogicalNodePropertyReaderToPhysical(logicalOperator, graph, physicalOperatorInfo);
    case LOGICAL_SCAN_REL_PROPERTY:
        return mapLogicalRelPropertyReaderToPhysical(logicalOperator, graph, physicalOperatorInfo);
    default:
        // should never happen.
        throw std::invalid_argument("Unsupported operator type.");
    }
}

unique_ptr<PhysicalOperator> mapLogicalScanNodeIDToPhysical(const LogicalOperator& logicalOperator,
    const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& scan = (const LogicalScanNodeID&)logicalOperator;
    auto morsel = make_shared<MorselDesc>(graph.getNumNodes(scan.label));
    physicalOperatorInfo.appendAsNewDataChunk(scan.nodeVarName);
    return make_unique<ScanNodeID<true>>(morsel);
}

unique_ptr<PhysicalOperator> mapLogicalExtendToPhysical(const LogicalOperator& logicalOperator,
    const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& extend = (const LogicalExtend&)logicalOperator;
    auto prevOperator =
        mapLogicalOperatorToPhysical(*logicalOperator.prevOperator, graph, physicalOperatorInfo);
    auto dataChunkPos = physicalOperatorInfo.getDataChunkPos(extend.boundNodeVarName);
    auto valueVectorPos = physicalOperatorInfo.getValueVectorPos(extend.boundNodeVarName);
    auto& catalog = graph.getCatalog();
    auto& relsStore = graph.getRelsStore();
    if (catalog.isSingleCaridinalityInDir(extend.relLabel, extend.direction)) {
        physicalOperatorInfo.appendAsNewValueVector(extend.nbrNodeVarName, dataChunkPos);
        return make_unique<AdjColumnExtend>(dataChunkPos, valueVectorPos,
            relsStore.getAdjColumn(extend.direction, extend.boundNodeVarLabel, extend.relLabel),
            move(prevOperator));
    } else {
        physicalOperatorInfo.appendAsNewDataChunk(extend.nbrNodeVarName);
        if (physicalOperatorInfo.dataChunkIsFlatVector[dataChunkPos]) {
            return make_unique<Extend>(dataChunkPos, valueVectorPos,
                relsStore.getAdjLists(extend.direction, extend.boundNodeVarLabel, extend.relLabel),
                move(prevOperator));
        } else {
            physicalOperatorInfo.setDataChunkAtPosAsFlat(dataChunkPos);
            return make_unique<FlattenAndExtend<true>>(dataChunkPos, valueVectorPos,
                relsStore.getAdjLists(extend.direction, extend.boundNodeVarLabel, extend.relLabel),
                move(prevOperator));
        }
    }
}

unique_ptr<PhysicalOperator> mapLogicalFilterToPhysical(const LogicalOperator& logicalOperator,
    const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& logicalFilter = (const LogicalFilter&)logicalOperator;
    auto prevOperator =
        mapLogicalOperatorToPhysical(*logicalOperator.prevOperator, graph, physicalOperatorInfo);
    auto dataChunks = prevOperator->getDataChunks();
    auto rootExpr = ExpressionMapper::mapToPhysical(
        logicalFilter.getRootLogicalExpression(), physicalOperatorInfo, *dataChunks);
    return make_unique<Filter>(move(rootExpr),
        dataChunks->getNumDataChunks() - 1 /* select over last extension */, move(prevOperator));
}

unique_ptr<PhysicalOperator> mapLogicalNodePropertyReaderToPhysical(
    const LogicalOperator& logicalOperator, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& scanProperty = (const LogicalScanNodeProperty&)logicalOperator;
    auto prevOperator =
        mapLogicalOperatorToPhysical(*logicalOperator.prevOperator, graph, physicalOperatorInfo);
    auto dataChunkPos = physicalOperatorInfo.getDataChunkPos(scanProperty.nodeVarName);
    auto valueVectorPos = physicalOperatorInfo.getValueVectorPos(scanProperty.nodeVarName);
    auto& catalog = graph.getCatalog();
    auto property =
        catalog.getNodePropertyKeyFromString(scanProperty.nodeLabel, scanProperty.propertyName);
    auto& nodesStore = graph.getNodesStore();
    physicalOperatorInfo.appendAsNewValueVector(
        scanProperty.nodeVarName + "." + scanProperty.propertyName, dataChunkPos);
    return make_unique<ScanNodeProperty>(dataChunkPos, valueVectorPos,
        nodesStore.getNodePropertyColumn(scanProperty.nodeLabel, property), move(prevOperator));
}

unique_ptr<PhysicalOperator> mapLogicalRelPropertyReaderToPhysical(
    const LogicalOperator& logicalOperator, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& scanProperty = (const LogicalScanRelProperty&)logicalOperator;
    auto prevOperator =
        mapLogicalOperatorToPhysical(*logicalOperator.prevOperator, graph, physicalOperatorInfo);
    auto inDataChunkPos = physicalOperatorInfo.getDataChunkPos(scanProperty.boundNodeVarName);
    auto inValueVectorPos = physicalOperatorInfo.getValueVectorPos(scanProperty.boundNodeVarName);
    auto outDataChunkPos = physicalOperatorInfo.getDataChunkPos(scanProperty.nbrNodeVarName);
    auto& catalog = graph.getCatalog();
    auto property =
        catalog.getRelPropertyKeyFromString(scanProperty.relLabel, scanProperty.propertyName);
    auto& relsStore = graph.getRelsStore();
    physicalOperatorInfo.appendAsNewValueVector(
        scanProperty.relName + "." + scanProperty.propertyName, outDataChunkPos);
    if (catalog.isSingleCaridinalityInDir(scanProperty.relLabel, scanProperty.direction)) {
        auto column = relsStore.getRelPropertyColumn(
            scanProperty.relLabel, scanProperty.boundNodeVarLabel, property);
        return make_unique<ScanRelProperty>(
            inDataChunkPos, inValueVectorPos, column, move(prevOperator));
    } else {
        auto lists = relsStore.getRelPropertyLists(scanProperty.direction,
            scanProperty.boundNodeVarLabel, scanProperty.relLabel, property);
        return make_unique<ReadRelPropertyList>(
            inDataChunkPos, inValueVectorPos, outDataChunkPos, lists, move(prevOperator));
    }
}

unique_ptr<PhysicalOperator> mapLogicalHashJoinToPhysical(const LogicalOperator& logicalOperator,
    const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& hashJoin = (const LogicalHashJoin&)logicalOperator;
    PhysicalOperatorsInfo probeSideOperatorInfo, buildSideOperatorInfo;
    auto probeSidePrevOperator =
        mapLogicalOperatorToPhysical(*hashJoin.prevOperator, graph, probeSideOperatorInfo);
    auto buildSidePrevOperator =
        mapLogicalOperatorToPhysical(*hashJoin.buildSidePrevOperator, graph, buildSideOperatorInfo);
    auto probeSideKeyDataChunkPos = probeSideOperatorInfo.getDataChunkPos(hashJoin.joinNodeVarName);
    auto probeSideKeyVectorPos = probeSideOperatorInfo.getValueVectorPos(hashJoin.joinNodeVarName);
    auto buildSideKeyDataChunkPos = buildSideOperatorInfo.getDataChunkPos(hashJoin.joinNodeVarName);
    auto buildSideKeyVectorPos = buildSideOperatorInfo.getValueVectorPos(hashJoin.joinNodeVarName);
    // Hash join data chunks construction
    // Probe side: 1) append the key data chunk as dataChunks[0].
    //             2) append other non-key data chunks.
    physicalOperatorInfo.appendAsNewDataChunk(
        probeSideOperatorInfo.vectorVariables[probeSideKeyDataChunkPos][0]);
    for (uint64_t i = 1; i < probeSideOperatorInfo.vectorVariables[probeSideKeyDataChunkPos].size();
         i++) {
        physicalOperatorInfo.appendAsNewValueVector(
            probeSideOperatorInfo.vectorVariables[probeSideKeyDataChunkPos][i], 0);
    }
    for (uint64_t i = 0; i < probeSideOperatorInfo.vectorVariables.size(); i++) {
        if (i == probeSideKeyDataChunkPos) {
            continue;
        }
        auto newDataChunkPos =
            physicalOperatorInfo.appendAsNewDataChunk(probeSideOperatorInfo.vectorVariables[i][0]);
        physicalOperatorInfo.dataChunkIsFlatVector[newDataChunkPos] =
            probeSideOperatorInfo.dataChunkIsFlatVector[i];
        for (uint64_t j = 1; j < probeSideOperatorInfo.vectorVariables[i].size(); j++) {
            physicalOperatorInfo.appendAsNewValueVector(
                probeSideOperatorInfo.vectorVariables[i][j], newDataChunkPos);
        }
    }
    // Build side: 1) append non-key vectors in the key data chunk into dataChunk[0].
    //             2) append flat non-key data chunks into dataChunks[0].
    //             3) append unflat non-key data chunks as new data chunks into dataChunks.
    auto& buildSideKeyDataChunk = buildSideOperatorInfo.vectorVariables[buildSideKeyDataChunkPos];
    for (uint64_t i = 0; i < buildSideKeyDataChunk.size(); i++) {
        if (i == buildSideKeyVectorPos) {
            continue;
        }
        physicalOperatorInfo.appendAsNewValueVector(buildSideKeyDataChunk[i], 0);
    }
    bool buildSideHasUnFlatDataChunk = false;
    for (uint64_t i = 0; i < buildSideOperatorInfo.vectorVariables.size(); i++) {
        if (i == buildSideKeyDataChunkPos) {
            continue;
        }
        if (buildSideOperatorInfo.dataChunkIsFlatVector[i]) {
            for (auto& variableName : buildSideOperatorInfo.vectorVariables[i]) {
                physicalOperatorInfo.appendAsNewValueVector(variableName, 0);
            }
        } else {
            auto newDataChunkPos = physicalOperatorInfo.appendAsNewDataChunk(
                buildSideOperatorInfo.vectorVariables[i][0]);
            for (uint64_t j = 1; j < buildSideOperatorInfo.vectorVariables[i].size(); j++) {
                physicalOperatorInfo.appendAsNewValueVector(
                    buildSideOperatorInfo.vectorVariables[i][j], newDataChunkPos);
            }
            buildSideHasUnFlatDataChunk = true;
        }
    }
    // Set dataChunks[0] as flat if there are unflat data chunks from the build side.
    if (buildSideHasUnFlatDataChunk) {
        physicalOperatorInfo.setDataChunkAtPosAsFlat(0);
    }

    auto hashJoinBuild = make_unique<HashJoinBuild>(
        buildSideKeyDataChunkPos, buildSideKeyVectorPos, move(buildSidePrevOperator));
    auto hashJoinSharedState =
        make_shared<HashJoinSharedState>(hashJoinBuild->numBytesForFixedTuplePart);
    hashJoinBuild->sharedState = hashJoinSharedState;
    auto hashJoinProbe = make_unique<HashJoinProbe<true>>(buildSideKeyDataChunkPos,
        buildSideKeyVectorPos, probeSideKeyDataChunkPos, probeSideKeyVectorPos, move(hashJoinBuild),
        move(probeSidePrevOperator));
    hashJoinProbe->sharedState = hashJoinSharedState;
    return hashJoinProbe;
}

} // namespace processor
} // namespace graphflow
