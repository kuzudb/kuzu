#include "src/processor/include/physical_plan/plan_mapper.h"

#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/filter/logical_filter.h"
#include "src/planner/include/logical_plan/operator/hash_join/logical_hash_join.h"
#include "src/planner/include/logical_plan/operator/property_reader/logical_node_property_reader.h"
#include "src/planner/include/logical_plan/operator/property_reader/logical_rel_property_reader.h"
#include "src/planner/include/logical_plan/operator/scan/logical_scan.h"
#include "src/processor/include/physical_plan/expression_mapper.h"
#include "src/processor/include/physical_plan/operator/column_reader/adj_column_extend.h"
#include "src/processor/include/physical_plan/operator/column_reader/node_property_column_reader.h"
#include "src/processor/include/physical_plan/operator/column_reader/rel_property_column_reader.h"
#include "src/processor/include/physical_plan/operator/filter/filter.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"
#include "src/processor/include/physical_plan/operator/list_reader/extend/adj_list_flatten_and_extend.h"
#include "src/processor/include/physical_plan/operator/list_reader/extend/adj_list_only_extend.h"
#include "src/processor/include/physical_plan/operator/list_reader/rel_property_list_reader.h"
#include "src/processor/include/physical_plan/operator/scan/physical_scan.h"
#include "src/processor/include/physical_plan/operator/sink/result_collector.h"

using namespace graphflow::planner;

namespace graphflow {
namespace processor {

static unique_ptr<PhysicalOperator> mapLogicalOperatorToPhysical(
    const LogicalOperator& logicalOperator, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo);

static unique_ptr<PhysicalOperator> mapLogicalScanToPhysical(const LogicalOperator& logicalOperator,
    const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo);

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
    case LOGICAL_SCAN:
        return mapLogicalScanToPhysical(logicalOperator, graph, physicalOperatorInfo);
    case LOGICAL_EXTEND:
        return mapLogicalExtendToPhysical(logicalOperator, graph, physicalOperatorInfo);
    case LOGICAL_FILTER:
        // warning: assumes flattening is to be taken care of by the enumerator and not the mapper.
        //          we do not append any flattening currently.
        return mapLogicalFilterToPhysical(logicalOperator, graph, physicalOperatorInfo);
    case LOGICAL_HASH_JOIN:
        return mapLogicalHashJoinToPhysical(logicalOperator, graph, physicalOperatorInfo);
    case LOGICAL_NODE_PROPERTY_READER:
        return mapLogicalNodePropertyReaderToPhysical(logicalOperator, graph, physicalOperatorInfo);
    case LOGICAL_REL_PROPERTY_READER:
        return mapLogicalRelPropertyReaderToPhysical(logicalOperator, graph, physicalOperatorInfo);
    default:
        // should never happen.
        throw std::invalid_argument("Unsupported operator type.");
    }
}

unique_ptr<PhysicalOperator> mapLogicalScanToPhysical(const LogicalOperator& logicalOperator,
    const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& scan = (const LogicalScan&)logicalOperator;
    auto morsel = make_shared<MorselDesc>(graph.getNumNodes(scan.label));
    physicalOperatorInfo.appendAsNewDataChunk(scan.nodeVarName);
    return make_unique<PhysicalScan<true>>(morsel);
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
            return make_unique<AdjListOnlyExtend>(dataChunkPos, valueVectorPos,
                relsStore.getAdjLists(extend.direction, extend.boundNodeVarLabel, extend.relLabel),
                move(prevOperator));
        } else {
            physicalOperatorInfo.setDataChunkAtPosAsFlat(dataChunkPos);
            return make_unique<AdjListFlattenAndExtend<true>>(dataChunkPos, valueVectorPos,
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
    auto& propertyReader = (const LogicalNodePropertyReader&)logicalOperator;
    auto prevOperator =
        mapLogicalOperatorToPhysical(*logicalOperator.prevOperator, graph, physicalOperatorInfo);
    auto dataChunkPos = physicalOperatorInfo.getDataChunkPos(propertyReader.nodeVarName);
    auto valueVectorPos = physicalOperatorInfo.getValueVectorPos(propertyReader.nodeVarName);
    auto& catalog = graph.getCatalog();
    auto label = catalog.getNodeLabelFromString(propertyReader.nodeLabel.c_str());
    auto property = catalog.getNodePropertyKeyFromString(label, propertyReader.propertyName);
    auto& nodesStore = graph.getNodesStore();
    physicalOperatorInfo.appendAsNewValueVector(
        propertyReader.nodeVarName + "." + propertyReader.propertyName, dataChunkPos);
    return make_unique<NodePropertyColumnReader>(dataChunkPos, valueVectorPos,
        nodesStore.getNodePropertyColumn(label, property), move(prevOperator));
}

unique_ptr<PhysicalOperator> mapLogicalRelPropertyReaderToPhysical(
    const LogicalOperator& logicalOperator, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& propertyReader = (const LogicalRelPropertyReader&)logicalOperator;
    auto prevOperator =
        mapLogicalOperatorToPhysical(*logicalOperator.prevOperator, graph, physicalOperatorInfo);
    auto inDataChunkPos = physicalOperatorInfo.getDataChunkPos(propertyReader.boundNodeVarName);
    auto inValueVectorPos = physicalOperatorInfo.getValueVectorPos(propertyReader.boundNodeVarName);
    auto outDataChunkPos = physicalOperatorInfo.getDataChunkPos(propertyReader.nbrNodeVarName);
    auto& catalog = graph.getCatalog();
    auto nodeLabel = catalog.getNodeLabelFromString(propertyReader.boundNodeVarLabel.c_str());
    auto label = catalog.getRelLabelFromString(propertyReader.relLabel.c_str());
    auto property = catalog.getRelPropertyKeyFromString(label, propertyReader.propertyName);
    auto& relsStore = graph.getRelsStore();
    physicalOperatorInfo.appendAsNewValueVector(
        propertyReader.relName + "." + propertyReader.propertyName, outDataChunkPos);
    if (catalog.isSingleCaridinalityInDir(label, propertyReader.direction)) {
        auto column = relsStore.getRelPropertyColumn(label, nodeLabel, property);
        return make_unique<RelPropertyColumnReader>(
            inDataChunkPos, inValueVectorPos, column, move(prevOperator));
    } else {
        auto lists =
            relsStore.getRelPropertyLists(propertyReader.direction, nodeLabel, label, property);
        return make_unique<RelPropertyListReader>(
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

    auto hashJoinSharedState = make_shared<HashJoinSharedState>();
    auto hashJoinBuild = make_unique<HashJoinBuild>(
        buildSideKeyDataChunkPos, buildSideKeyVectorPos, move(buildSidePrevOperator));
    hashJoinSharedState->hashTable = hashJoinBuild->initializeHashTable();
    hashJoinBuild->sharedState = hashJoinSharedState;
    auto hashJoinProbe = make_unique<HashJoinProbe<true>>(buildSideKeyDataChunkPos,
        buildSideKeyVectorPos, probeSideKeyDataChunkPos, probeSideKeyVectorPos, move(hashJoinBuild),
        move(probeSidePrevOperator));
    hashJoinProbe->sharedState = hashJoinSharedState;
    return hashJoinProbe;
}

} // namespace processor
} // namespace graphflow
