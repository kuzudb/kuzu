#include "src/processor/include/physical_plan/plan_mapper.h"

#include <set>

#include "src/planner/include/logical_plan/operator/crud/logical_crud_node.h"
#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/filter/logical_filter.h"
#include "src/planner/include/logical_plan/operator/hash_join/logical_hash_join.h"
#include "src/planner/include/logical_plan/operator/load_csv/logical_load_csv.h"
#include "src/planner/include/logical_plan/operator/projection/logical_projection.h"
#include "src/planner/include/logical_plan/operator/scan_node_id/logical_scan_node_id.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_node_property.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_rel_property.h"
#include "src/processor/include/physical_plan/expression_mapper.h"
#include "src/processor/include/physical_plan/operator/crud/create_node.h"
#include "src/processor/include/physical_plan/operator/crud/delete_node.h"
#include "src/processor/include/physical_plan/operator/crud/update_node.h"
#include "src/processor/include/physical_plan/operator/filter/filter.h"
#include "src/processor/include/physical_plan/operator/flatten/flatten.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"
#include "src/processor/include/physical_plan/operator/load_csv/load_csv.h"
#include "src/processor/include/physical_plan/operator/physical_operator_info.h"
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

static unique_ptr<PhysicalOperator> mapLogicalOperatorToPhysical(
    const LogicalOperator& logicalOperator, Transaction* transactionPtr, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo);

static unique_ptr<PhysicalOperator> mapLogicalScanNodeIDToPhysical(
    const LogicalOperator& logicalOperator, Transaction* transactionPtr, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo);

static unique_ptr<PhysicalOperator> mapLogicalExtendToPhysical(
    const LogicalOperator& logicalOperator, Transaction* transactionPtr, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo);

static unique_ptr<PhysicalOperator> mapLogicalFilterToPhysical(
    const LogicalOperator& logicalOperator, Transaction* transactionPtr, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo);

static unique_ptr<PhysicalOperator> mapLogicalProjectionToPhysical(
    const LogicalOperator& logicalOperator, Transaction* transactionPtr, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo);

static unique_ptr<PhysicalOperator> mapLogicalNodePropertyReaderToPhysical(
    const LogicalOperator& logicalOperator, Transaction* transactionPtr, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo);

static unique_ptr<PhysicalOperator> mapLogicalRelPropertyReaderToPhysical(
    const LogicalOperator& logicalOperator, Transaction* transactionPtr, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo);

static unique_ptr<PhysicalOperator> mapLogicalHashJoinToPhysical(
    const LogicalOperator& logicalOperator, Transaction* transactionPtr, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo);

static unique_ptr<PhysicalOperator> mapLogicalLoadCSVToPhysical(
    const LogicalOperator& logicalOperator, Transaction* transactionPtr, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo);

static unique_ptr<PhysicalOperator> mapLogicalCRUDNodeToPhysical(
    const LogicalOperator& logicalOperator, Transaction* transactionPtr, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo);

// helper functions used by mapLogical{Filter/Projection}ToPhysical.
static unique_ptr<PhysicalOperator> appendFlattenOperatorsIfNecessary(
    const Expression& logicalRootExpr, PhysicalOperatorsInfo& physicalOperatorInfo,
    unique_ptr<PhysicalOperator> prevOperator);

static uint64_t getDependentUnflatDataChunkPos(
    const Expression& logicalRootExpr, PhysicalOperatorsInfo& physicalOperatorInfo);

unique_ptr<PhysicalPlan> PlanMapper::mapToPhysical(
    unique_ptr<LogicalPlan> logicalPlan, Transaction* transactionPtr, const Graph& graph) {
    auto physicalOperatorInfo = PhysicalOperatorsInfo();
    auto resultCollector = make_unique<ResultCollector>(mapLogicalOperatorToPhysical(
        logicalPlan->getLastOperator(), transactionPtr, graph, physicalOperatorInfo));
    return make_unique<PhysicalPlan>(move(resultCollector));
}

unique_ptr<PhysicalOperator> mapLogicalOperatorToPhysical(const LogicalOperator& logicalOperator,
    Transaction* transactionPtr, const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto operatorType = logicalOperator.getLogicalOperatorType();
    switch (operatorType) {
    case LOGICAL_SCAN_NODE_ID:
        return mapLogicalScanNodeIDToPhysical(
            logicalOperator, transactionPtr, graph, physicalOperatorInfo);
    case LOGICAL_EXTEND:
        return mapLogicalExtendToPhysical(
            logicalOperator, transactionPtr, graph, physicalOperatorInfo);
    case LOGICAL_FILTER:
        return mapLogicalFilterToPhysical(
            logicalOperator, transactionPtr, graph, physicalOperatorInfo);
    case LOGICAL_PROJECTION:
        return mapLogicalProjectionToPhysical(
            logicalOperator, transactionPtr, graph, physicalOperatorInfo);
    case LOGICAL_HASH_JOIN:
        return mapLogicalHashJoinToPhysical(
            logicalOperator, transactionPtr, graph, physicalOperatorInfo);
    case LOGICAL_SCAN_NODE_PROPERTY:
        return mapLogicalNodePropertyReaderToPhysical(
            logicalOperator, transactionPtr, graph, physicalOperatorInfo);
    case LOGICAL_SCAN_REL_PROPERTY:
        return mapLogicalRelPropertyReaderToPhysical(
            logicalOperator, transactionPtr, graph, physicalOperatorInfo);
    case LOGICAL_LOAD_CSV:
        return mapLogicalLoadCSVToPhysical(
            logicalOperator, transactionPtr, graph, physicalOperatorInfo);
    case LOGICAL_CREATE_NODE:
    case LOGICAL_UPDATE_NODE:
        return mapLogicalCRUDNodeToPhysical(
            logicalOperator, transactionPtr, graph, physicalOperatorInfo);
    default:
        assert(false);
    }
}

unique_ptr<PhysicalOperator> mapLogicalScanNodeIDToPhysical(const LogicalOperator& logicalOperator,
    Transaction* transactionPtr, const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& scan = (const LogicalScanNodeID&)logicalOperator;
    auto morsel = make_shared<MorselsDesc>(scan.label, graph.getNumNodes(scan.label));
    physicalOperatorInfo.appendAsNewDataChunk(scan.nodeID);
    return make_unique<ScanNodeID<true>>(morsel);
}

unique_ptr<PhysicalOperator> mapLogicalExtendToPhysical(const LogicalOperator& logicalOperator,
    Transaction* transactionPtr, const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& extend = (const LogicalExtend&)logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(
        *logicalOperator.prevOperator, transactionPtr, graph, physicalOperatorInfo);
    auto dataChunkPos = physicalOperatorInfo.getDataChunkPos(extend.boundNodeID);
    auto valueVectorPos = physicalOperatorInfo.getValueVectorPos(extend.boundNodeID);
    auto& relsStore = graph.getRelsStore();
    if (extend.isColumn) {
        assert(extend.lowerBound == extend.upperBound && extend.lowerBound == 1);
        physicalOperatorInfo.appendAsNewValueVector(extend.nbrNodeID, dataChunkPos);
        return make_unique<AdjColumnExtend>(dataChunkPos, valueVectorPos,
            relsStore.getAdjColumn(extend.direction, extend.boundNodeLabel, extend.relLabel),
            move(prevOperator));
    } else {
        if (!physicalOperatorInfo.dataChunkPosToIsFlat[dataChunkPos]) {
            physicalOperatorInfo.dataChunkPosToIsFlat[dataChunkPos] = true;
            prevOperator = make_unique<Flatten>(dataChunkPos, move(prevOperator));
        }
        physicalOperatorInfo.appendAsNewDataChunk(extend.nbrNodeID);
        auto adjLists =
            relsStore.getAdjLists(extend.direction, extend.boundNodeLabel, extend.relLabel);
        if (extend.lowerBound == 1 && extend.lowerBound == extend.upperBound) {
            return make_unique<AdjListExtend<true>>(
                dataChunkPos, valueVectorPos, adjLists, move(prevOperator));
        } else {
            return make_unique<FrontierExtend<true>>(dataChunkPos, valueVectorPos, adjLists,
                extend.lowerBound, extend.upperBound, move(prevOperator));
        }
    }
}

unique_ptr<PhysicalOperator> mapLogicalFilterToPhysical(const LogicalOperator& logicalOperator,
    Transaction* transactionPtr, const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& logicalFilter = (const LogicalFilter&)logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(
        *logicalOperator.prevOperator, transactionPtr, graph, physicalOperatorInfo);
    const auto& logicalRootExpr = logicalFilter.getRootExpression();
    prevOperator = appendFlattenOperatorsIfNecessary(
        logicalRootExpr, physicalOperatorInfo, move(prevOperator));
    auto dataChunkToSelectPos =
        getDependentUnflatDataChunkPos(logicalRootExpr, physicalOperatorInfo);
    auto physicalRootExpr = ExpressionMapper::mapToPhysical(
        logicalRootExpr, physicalOperatorInfo, *prevOperator->getResultSet());
    if (prevOperator->operatorType == FLATTEN) {
        return make_unique<Filter<true /* isAfterFlatten */>>(
            move(physicalRootExpr), dataChunkToSelectPos, move(prevOperator));
    } else {
        return make_unique<Filter<false /* isAfterFlatten */>>(
            move(physicalRootExpr), dataChunkToSelectPos, move(prevOperator));
    }
}

unique_ptr<PhysicalOperator> mapLogicalProjectionToPhysical(const LogicalOperator& logicalOperator,
    Transaction* transactionPtr, const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto prevOperator = mapLogicalOperatorToPhysical(
        *logicalOperator.prevOperator, transactionPtr, graph, physicalOperatorInfo);
    auto& logicalProjection = (const LogicalProjection&)logicalOperator;
    // We obtain the number of dataChunks from the previous operator as the projection operator
    // creates new dataChunks and we will be emptying the physicalOperatorInfo object.
    auto numInputDataChunks = physicalOperatorInfo.vectorVariables.size();

    // We append flatten(s) as necessary and map each logical expression to a expression_evaluator
    // one.
    auto expressionEvaluators = make_unique<vector<unique_ptr<ExpressionEvaluator>>>();
    for (const auto& logicalRootExpr : logicalProjection.expressionsToProject) {
        prevOperator = appendFlattenOperatorsIfNecessary(
            *logicalRootExpr, physicalOperatorInfo, move(prevOperator));
        expressionEvaluators->push_back(ExpressionMapper::mapToPhysical(
            *logicalRootExpr, physicalOperatorInfo, *prevOperator->getResultSet()));
    }

    // We collect the input data chunk positions for the vectors in the expressions.
    vector<string> expressionName;
    vector<uint64_t> exprResultInDataChunkPos;
    for (const auto& logicalRootExpr : logicalProjection.expressionsToProject) {
        auto name = logicalRootExpr->getAliasElseRawExpression();
        expressionName.push_back(name);
        auto isLeafVariableExpr = isExpressionLeafVariable(logicalRootExpr->expressionType);
        exprResultInDataChunkPos.push_back(
            isLeafVariableExpr ?
                physicalOperatorInfo.getDataChunkPos(name) :
                getDependentUnflatDataChunkPos(*logicalRootExpr, physicalOperatorInfo));
    }

    // We map the input data chunk positions from the prev operator to new normalized ones from 0 to
    // m, where m is the number of output data chunk positions.
    physicalOperatorInfo.clear();
    vector<uint64_t> exprResultOutDataChunkPos;
    exprResultOutDataChunkPos.reserve(expressionEvaluators->size());
    auto currNumOutDataChunks = 0;
    unordered_map<uint64_t, uint64_t> inToOutDataChunkPosMap;
    for (auto i = 0u; i < expressionEvaluators->size(); i++) {
        auto it = inToOutDataChunkPosMap.find(exprResultInDataChunkPos[i]);
        if (it != end(inToOutDataChunkPosMap)) {
            exprResultOutDataChunkPos[i] = inToOutDataChunkPosMap.at(exprResultInDataChunkPos[i]);
            physicalOperatorInfo.appendAsNewValueVector(
                expressionName[i], exprResultOutDataChunkPos[i]);
        } else {
            exprResultOutDataChunkPos[i] = currNumOutDataChunks++;
            inToOutDataChunkPosMap.insert(
                {exprResultInDataChunkPos[i], exprResultOutDataChunkPos[i]});
            physicalOperatorInfo.appendAsNewDataChunk(expressionName[i]);
        }
    }

    // We collect the discarded dataChunk positions in the input data chunks to obtain their
    // multiplicity in the projection operation.
    vector<uint64_t> discardedDataChunkPos;
    for (auto i = 0u; i < numInputDataChunks; i++) {
        auto it = find(exprResultOutDataChunkPos.begin(), exprResultOutDataChunkPos.end(), i);
        if (it == exprResultOutDataChunkPos.end()) {
            discardedDataChunkPos.push_back(i);
        }
    }
    return make_unique<Projection>(move(expressionEvaluators), exprResultOutDataChunkPos,
        discardedDataChunkPos, move(prevOperator));
}

unique_ptr<PhysicalOperator> appendFlattenOperatorsIfNecessary(const Expression& logicalRootExpr,
    PhysicalOperatorsInfo& physicalOperatorInfo, unique_ptr<PhysicalOperator> prevOperator) {
    pair<unique_ptr<PhysicalOperator>, uint64_t> result;
    set<uint64_t> unflatDataChunkPosSet;
    for (auto& propertyExpression : logicalRootExpr.getIncludedPropertyExpressions()) {
        auto pos = physicalOperatorInfo.getDataChunkPos(propertyExpression->variableName);
        if (!physicalOperatorInfo.dataChunkPosToIsFlat[pos]) {
            unflatDataChunkPosSet.insert(pos);
        }
    }
    if (!unflatDataChunkPosSet.empty()) {
        vector<uint64_t> unflatDataChunkPos(
            unflatDataChunkPosSet.begin(), unflatDataChunkPosSet.end());
        if (unflatDataChunkPosSet.size() > 1) {
            for (auto i = 0u; i < unflatDataChunkPos.size() - 1; i++) {
                auto pos = unflatDataChunkPos[i];
                prevOperator = make_unique<Flatten>(pos, move(prevOperator));
                physicalOperatorInfo.dataChunkPosToIsFlat[pos] = true;
            }
        }
    }
    return prevOperator;
}

// The function finds the data chunk position of the unflat vectors in the expression, if
// all vectors in the expressions are flat, we return null as UINT64_MAX.
uint64_t getDependentUnflatDataChunkPos(
    const Expression& logicalRootExpr, PhysicalOperatorsInfo& physicalOperatorInfo) {
    for (auto& propertyExpression : logicalRootExpr.getIncludedPropertyExpressions()) {
        auto pos = physicalOperatorInfo.getDataChunkPos(propertyExpression->variableName);
        if (!physicalOperatorInfo.dataChunkPosToIsFlat[pos]) {
            return pos;
        }
    }
    return UINT64_MAX /* null */;
}

unique_ptr<PhysicalOperator> mapLogicalNodePropertyReaderToPhysical(
    const LogicalOperator& logicalOperator, Transaction* transactionPtr, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& scanProperty = (const LogicalScanNodeProperty&)logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(
        *logicalOperator.prevOperator, transactionPtr, graph, physicalOperatorInfo);
    auto dataChunkPos = physicalOperatorInfo.getDataChunkPos(scanProperty.nodeID);
    auto valueVectorPos = physicalOperatorInfo.getValueVectorPos(scanProperty.nodeID);
    physicalOperatorInfo.appendAsNewValueVector(scanProperty.propertyVariableName, dataChunkPos);
    if (scanProperty.isUnstructuredProperty) {
        auto lists = graph.getNodesStore().getNodeUnstrPropertyLists(scanProperty.nodeLabel);
        return make_unique<ScanUnstructuredProperty>(
            dataChunkPos, valueVectorPos, scanProperty.propertyKey, lists, move(prevOperator));
    }
    auto column = graph.getNodesStore().getNodePropertyColumn(
        scanProperty.nodeLabel, scanProperty.propertyKey);
    return make_unique<ScanStructuredProperty>(
        dataChunkPos, valueVectorPos, column, move(prevOperator));
}

unique_ptr<PhysicalOperator> mapLogicalRelPropertyReaderToPhysical(
    const LogicalOperator& logicalOperator, Transaction* transactionPtr, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& scanProperty = (const LogicalScanRelProperty&)logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(
        *logicalOperator.prevOperator, transactionPtr, graph, physicalOperatorInfo);
    auto inDataChunkPos = physicalOperatorInfo.getDataChunkPos(scanProperty.boundNodeID);
    auto inValueVectorPos = physicalOperatorInfo.getValueVectorPos(scanProperty.boundNodeID);
    auto outDataChunkPos = physicalOperatorInfo.getDataChunkPos(scanProperty.nbrNodeID);
    physicalOperatorInfo.appendAsNewValueVector(scanProperty.propertyVariableName, outDataChunkPos);
    if (scanProperty.isColumn) {
        auto column = graph.getRelsStore().getRelPropertyColumn(
            scanProperty.relLabel, scanProperty.boundNodeLabel, scanProperty.propertyKey);
        return make_unique<ScanStructuredProperty>(
            inDataChunkPos, inValueVectorPos, column, move(prevOperator));
    }
    auto lists = graph.getRelsStore().getRelPropertyLists(scanProperty.direction,
        scanProperty.boundNodeLabel, scanProperty.relLabel, scanProperty.propertyKey);
    return make_unique<ReadRelPropertyList>(
        inDataChunkPos, inValueVectorPos, outDataChunkPos, lists, move(prevOperator));
}

unique_ptr<PhysicalOperator> mapLogicalHashJoinToPhysical(const LogicalOperator& logicalOperator,
    Transaction* transactionPtr, const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& hashJoin = (const LogicalHashJoin&)logicalOperator;
    PhysicalOperatorsInfo probeSideOperatorInfo, buildSideOperatorInfo;
    auto probeSidePrevOperator = mapLogicalOperatorToPhysical(
        *hashJoin.prevOperator, transactionPtr, graph, probeSideOperatorInfo);
    auto buildSidePrevOperator = mapLogicalOperatorToPhysical(
        *hashJoin.buildSidePrevOperator, transactionPtr, graph, buildSideOperatorInfo);
    auto probeSideKeyDataChunkPos = probeSideOperatorInfo.getDataChunkPos(hashJoin.joinNodeID);
    auto probeSideKeyVectorPos = probeSideOperatorInfo.getValueVectorPos(hashJoin.joinNodeID);
    auto buildSideKeyDataChunkPos = buildSideOperatorInfo.getDataChunkPos(hashJoin.joinNodeID);
    auto buildSideKeyVectorPos = buildSideOperatorInfo.getValueVectorPos(hashJoin.joinNodeID);
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
        physicalOperatorInfo.dataChunkPosToIsFlat[newDataChunkPos] =
            probeSideOperatorInfo.dataChunkPosToIsFlat[i];
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
        if (buildSideOperatorInfo.dataChunkPosToIsFlat[i]) {
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
        physicalOperatorInfo.dataChunkPosToIsFlat[0] = true;
    }

    auto hashJoinBuild = make_unique<HashJoinBuild>(buildSideKeyDataChunkPos, buildSideKeyVectorPos,
        buildSideOperatorInfo.dataChunkPosToIsFlat, move(buildSidePrevOperator));
    auto hashJoinSharedState =
        make_shared<HashJoinSharedState>(hashJoinBuild->numBytesForFixedTuplePart);
    hashJoinBuild->sharedState = hashJoinSharedState;
    auto hashJoinProbe = make_unique<HashJoinProbe<true>>(buildSideKeyDataChunkPos,
        buildSideKeyVectorPos, buildSideOperatorInfo.dataChunkPosToIsFlat, probeSideKeyDataChunkPos,
        probeSideKeyVectorPos, move(hashJoinBuild), move(probeSidePrevOperator));
    hashJoinProbe->sharedState = hashJoinSharedState;
    return hashJoinProbe;
}

unique_ptr<PhysicalOperator> mapLogicalLoadCSVToPhysical(const LogicalOperator& logicalOperator,
    Transaction* transactionPtr, const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& logicalLoadCSV = (const LogicalLoadCSV&)logicalOperator;
    auto& columnInfo = logicalLoadCSV.csvColumnVariableInfo;
    assert(!columnInfo.empty());
    vector<DataType> csvColumnDataTypes;
    uint64_t dataChunkPos;
    for (auto i = 0u; i < columnInfo.size(); i++) {
        if (0u == i) {
            dataChunkPos = physicalOperatorInfo.appendAsNewDataChunk(columnInfo[0].first);
        } else {
            physicalOperatorInfo.appendAsNewValueVector(columnInfo[i].first, dataChunkPos);
        }
        csvColumnDataTypes.push_back(columnInfo[i].second);
    }
    return make_unique<LoadCSV>(
        logicalLoadCSV.path, logicalLoadCSV.tokenSeparator, csvColumnDataTypes);
}

unique_ptr<PhysicalOperator> mapLogicalCRUDNodeToPhysical(const LogicalOperator& logicalOperator,
    Transaction* transactionPtr, const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& logicalCRUDNode = (const LogicalCRUDNode&)logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(
        *logicalOperator.prevOperator, transactionPtr, graph, physicalOperatorInfo);
    auto& catalog = graph.getCatalog();
    auto& propertyKeyMap = catalog.getPropertyKeyMapForNodeLabel(logicalCRUDNode.nodeLabel);
    vector<BaseColumn*> nodePropertyColumns(propertyKeyMap.size());
    for (auto& entry : propertyKeyMap) {
        nodePropertyColumns[entry.second.idx] = graph.getNodesStore().getNodePropertyColumn(
            logicalCRUDNode.nodeLabel, entry.second.idx);
    }
    auto& propertyKeyToColVarMap = logicalCRUDNode.propertyKeyToCSVColumnVariableMap;
    vector<uint32_t> propertyKeyVectorPos(propertyKeyMap.size(), -1);
    uint32_t dataChunkPos;
    for (auto& entry : propertyKeyToColVarMap) {
        propertyKeyVectorPos[entry.first] = physicalOperatorInfo.getValueVectorPos(entry.second);
        dataChunkPos = physicalOperatorInfo.getDataChunkPos(entry.second);
    }
    if (LOGICAL_CREATE_NODE == logicalCRUDNode.getLogicalOperatorType()) {
        return make_unique<CreateNode>(dataChunkPos, transactionPtr, move(propertyKeyVectorPos),
            logicalCRUDNode.nodeLabel, move(nodePropertyColumns),
            graph.getNumNodes(logicalCRUDNode.nodeLabel), move(prevOperator));
    } else if (LOGICAL_UPDATE_NODE == logicalCRUDNode.getLogicalOperatorType()) {
        return make_unique<UpdateNode>(dataChunkPos, transactionPtr, move(propertyKeyVectorPos),
            logicalCRUDNode.nodeLabel, move(nodePropertyColumns),
            graph.getNumNodes(logicalCRUDNode.nodeLabel), move(prevOperator));
    } else if (LOGICAL_DELETE_NODE == logicalCRUDNode.getLogicalOperatorType()) {
        return make_unique<DeleteNode>(dataChunkPos, transactionPtr, move(propertyKeyVectorPos),
            logicalCRUDNode.nodeLabel, move(nodePropertyColumns),
            graph.getNumNodes(logicalCRUDNode.nodeLabel), move(prevOperator));
    }
    assert(false);
}

} // namespace processor
} // namespace graphflow
