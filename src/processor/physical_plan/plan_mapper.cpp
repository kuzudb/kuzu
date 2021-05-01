#include "src/processor/include/physical_plan/plan_mapper.h"

#include <set>

#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/filter/logical_filter.h"
#include "src/planner/include/logical_plan/operator/hash_join/logical_hash_join.h"
#include "src/planner/include/logical_plan/operator/projection/logical_projection.h"
#include "src/planner/include/logical_plan/operator/scan_node_id/logical_scan_node_id.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_node_property.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_rel_property.h"
#include "src/processor/include/physical_plan/expression_mapper.h"
#include "src/processor/include/physical_plan/operator/filter/filter.h"
#include "src/processor/include/physical_plan/operator/flatten/flatten.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"
#include "src/processor/include/physical_plan/operator/projection/projection.h"
#include "src/processor/include/physical_plan/operator/read_list/adj_list_extend.h"
#include "src/processor/include/physical_plan/operator/read_list/read_rel_property_list.h"
#include "src/processor/include/physical_plan/operator/scan_attribute/adj_column_extend.h"
#include "src/processor/include/physical_plan/operator/scan_attribute/scan_structured_property.h"
#include "src/processor/include/physical_plan/operator/scan_attribute/scan_unstructured_property.h"
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

static unique_ptr<PhysicalOperator> mapLogicalProjectionToPhysical(
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

static unique_ptr<PhysicalOperator> appendFlattenOperatorsIfNecessary(
    const LogicalExpression& logicalRootExpr, PhysicalOperatorsInfo& physicalOperatorInfo,
    unique_ptr<PhysicalOperator> prevOperator, uint64_t& dataChunkPos, bool& appendedFlatten);

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
    case LOGICAL_SCAN_NODE_ID:
        return mapLogicalScanNodeIDToPhysical(logicalOperator, graph, physicalOperatorInfo);
    case LOGICAL_EXTEND:
        return mapLogicalExtendToPhysical(logicalOperator, graph, physicalOperatorInfo);
    case LOGICAL_FILTER:
        return mapLogicalFilterToPhysical(logicalOperator, graph, physicalOperatorInfo);
    case LOGICAL_PROJECTION:
        // TODO: We do not handle flattening for projections currently.
        return mapLogicalProjectionToPhysical(logicalOperator, graph, physicalOperatorInfo);
    case LOGICAL_HASH_JOIN:
        return mapLogicalHashJoinToPhysical(logicalOperator, graph, physicalOperatorInfo);
    case LOGICAL_SCAN_NODE_PROPERTY:
        return mapLogicalNodePropertyReaderToPhysical(logicalOperator, graph, physicalOperatorInfo);
    case LOGICAL_SCAN_REL_PROPERTY:
        return mapLogicalRelPropertyReaderToPhysical(logicalOperator, graph, physicalOperatorInfo);
    default:
        assert(false);
    }
}

unique_ptr<PhysicalOperator> mapLogicalScanNodeIDToPhysical(const LogicalOperator& logicalOperator,
    const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& scan = (const LogicalScanNodeID&)logicalOperator;
    auto morsel = make_shared<MorselsDesc>(scan.label, graph.getNumNodes(scan.label));
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
        if (!physicalOperatorInfo.dataChunkIsFlatVector[dataChunkPos]) {
            physicalOperatorInfo.dataChunkIsFlatVector[dataChunkPos] = true;
            prevOperator = make_unique<Flatten>(dataChunkPos, move(prevOperator));
        }
        physicalOperatorInfo.appendAsNewDataChunk(extend.nbrNodeVarName);
        return make_unique<AdjListExtend<true>>(dataChunkPos, valueVectorPos,
            relsStore.getAdjLists(extend.direction, extend.boundNodeVarLabel, extend.relLabel),
            move(prevOperator));
    }
}

unique_ptr<PhysicalOperator> mapLogicalFilterToPhysical(const LogicalOperator& logicalOperator,
    const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& logicalFilter = (const LogicalFilter&)logicalOperator;
    auto prevOperator =
        mapLogicalOperatorToPhysical(*logicalOperator.prevOperator, graph, physicalOperatorInfo);
    auto dataChunks = prevOperator->getDataChunks();
    const auto& logicalRootExpr = logicalFilter.getRootLogicalExpression();
    uint64_t dataChunkToSelectPos;
    bool appendedFlatten;
    prevOperator = appendFlattenOperatorsIfNecessary(logicalRootExpr, physicalOperatorInfo,
        move(prevOperator), dataChunkToSelectPos, appendedFlatten);
    auto physicalRootExpr = ExpressionMapper::mapToPhysical(
        logicalRootExpr, physicalOperatorInfo, *prevOperator->getDataChunks());
    if (appendedFlatten) {
        return make_unique<Filter<true /* isAfterFlatten */>>(
            move(physicalRootExpr), dataChunkToSelectPos, move(prevOperator));
    } else {
        return make_unique<Filter<false /* isAfterFlatten */>>(
            move(physicalRootExpr), dataChunkToSelectPos, move(prevOperator));
    }
}

unique_ptr<PhysicalOperator> appendFlattenOperatorsIfNecessary(
    const LogicalExpression& logicalRootExpr, PhysicalOperatorsInfo& physicalOperatorInfo,
    unique_ptr<PhysicalOperator> prevOperator, uint64_t& dataChunkPos, bool& appendedFlatten) {
    set<uint64_t> unflatDataChunkPosSet;
    for (auto& property : logicalRootExpr.getIncludedProperties()) {
        auto pos = physicalOperatorInfo.getDataChunkPos(property);
        if (!physicalOperatorInfo.dataChunkIsFlatVector[pos]) {
            unflatDataChunkPosSet.insert(pos);
        }
    }
    appendedFlatten = false;
    if (!unflatDataChunkPosSet.empty()) {
        vector<uint64_t> unflatDataChunkPos(
            unflatDataChunkPosSet.begin(), unflatDataChunkPosSet.end());
        if (unflatDataChunkPosSet.size() > 1) {
            appendedFlatten = true;
            for (auto i = 0u; i < unflatDataChunkPos.size() - 1; i++) {
                auto pos = unflatDataChunkPos[i];
                prevOperator = make_unique<Flatten>(pos, move(prevOperator));
                physicalOperatorInfo.dataChunkIsFlatVector[pos] = true;
            }
        }
        // We only need to set the dataChunkPos if at least one dataChunk is unflat.
        dataChunkPos = unflatDataChunkPos[unflatDataChunkPos.size() - 1];
        return prevOperator;
    }
    return prevOperator;
}

// For each expression, map its result vector to the input dataChunks positions:
// 1) Property expressions will simply hold references to inputs, so their result are in the same
// data chunk as their input.
// 2) For non-property expressions: if the expression contains a input vector from a unflat data
// chunk, the result will be in the unflat data chunk; else, the result can be in any of the data
// chunks of the properties. We set it to the last property we read when we call
// getIncludedProperties().
static vector<vector<uint64_t>> mapProjectionExpressionsToDataChunkPos(DataChunks& dataChunks,
    vector<shared_ptr<LogicalExpression>> expressionsToProject,
    PhysicalOperatorsInfo& physicalOperatorInfo) {
    vector<vector<uint64_t>> expressionPosPerDataChunk;
    expressionPosPerDataChunk.resize(dataChunks.getNumDataChunks());
    for (uint64_t i = 0; i < expressionsToProject.size(); i++) {
        auto expression = expressionsToProject[i];
        if (expression->expressionType == PROPERTY) {
            auto exprVariable = expression->variableName;
            auto exprDataChunkPos = physicalOperatorInfo.getDataChunkPos(exprVariable);
            expressionPosPerDataChunk[exprDataChunkPos].push_back(i);
        } else {
            auto includedProps = expression->getIncludedProperties();
            int64_t exprDataChunkPos = -1;
            for (auto& prop : includedProps) {
                auto propDataChunkPos = physicalOperatorInfo.getDataChunkPos(prop);
                exprDataChunkPos = propDataChunkPos;
                if (!physicalOperatorInfo.dataChunkIsFlatVector[propDataChunkPos]) {
                    break; // Break here as we found a unflat data chunk
                }
            }
            if (exprDataChunkPos == -1) {
                throw invalid_argument(
                    "Unable to find data chunk position for the result vector of expression " +
                    expression->variableName);
            }
            expressionPosPerDataChunk[exprDataChunkPos].push_back(i);
        }
    }
    return expressionPosPerDataChunk;
}

// todo: add flatten operator
unique_ptr<PhysicalOperator> mapLogicalProjectionToPhysical(const LogicalOperator& logicalOperator,
    const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& logicalProjection = (const LogicalProjection&)logicalOperator;
    auto prevOperator =
        mapLogicalOperatorToPhysical(*logicalOperator.prevOperator, graph, physicalOperatorInfo);
    auto dataChunks = prevOperator->getDataChunks();

    auto expressionPosPerInDataChunk = mapProjectionExpressionsToDataChunkPos(
        *dataChunks, logicalProjection.expressionsToProject, physicalOperatorInfo);

    // Map logical expressions to physical ones
    vector<unique_ptr<PhysicalExpression>> physicalExpressions;
    for (auto& expression : logicalProjection.expressionsToProject) {
        physicalExpressions.push_back(
            ExpressionMapper::mapToPhysical(*expression, physicalOperatorInfo, *dataChunks));
    }

    // Update physical operator info
    physicalOperatorInfo.clear();
    for (uint64_t i = 0; i < expressionPosPerInDataChunk.size(); i++) {
        if (expressionPosPerInDataChunk[i].empty()) { // Skip empty (projected out) data chunks that
                                                      // contain no expression outputs
            continue;
        }
        auto outDataChunkPos = physicalOperatorInfo.appendAsNewDataChunk(
            logicalProjection.expressionsToProject[expressionPosPerInDataChunk[i][0]]
                ->rawExpression);
        for (uint64_t j = 1; j < expressionPosPerInDataChunk[i].size(); j++) {
            physicalOperatorInfo.appendAsNewValueVector(
                logicalProjection.expressionsToProject[expressionPosPerInDataChunk[i][j]]
                    ->rawExpression,
                outDataChunkPos);
        }
        physicalOperatorInfo.dataChunkIsFlatVector[outDataChunkPos] =
            dataChunks->getDataChunk(i)->state->isFlat();
    }

    return make_unique<Projection>(
        move(physicalExpressions), expressionPosPerInDataChunk, move(prevOperator));
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
    auto& nodesStore = graph.getNodesStore();
    physicalOperatorInfo.appendAsNewValueVector(
        scanProperty.nodeVarName + "." + scanProperty.propertyName, dataChunkPos);
    if (catalog.containNodeProperty(scanProperty.nodeLabel, scanProperty.propertyName)) {
        auto property =
            catalog.getNodePropertyKeyFromString(scanProperty.nodeLabel, scanProperty.propertyName);
        return make_unique<ScanStructuredProperty>(dataChunkPos, valueVectorPos,
            nodesStore.getNodePropertyColumn(scanProperty.nodeLabel, property), move(prevOperator));
    }
    auto property = catalog.getUnstrNodePropertyKeyFromString(
        scanProperty.nodeLabel, scanProperty.propertyName);
    return make_unique<ScanUnstructuredProperty>(dataChunkPos, valueVectorPos, property,
        nodesStore.getNodeUnstrPropertyLists(scanProperty.nodeLabel), move(prevOperator));
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
        return make_unique<ScanStructuredProperty>(
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
        physicalOperatorInfo.dataChunkIsFlatVector[0] = true;
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
