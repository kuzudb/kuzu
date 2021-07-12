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

static uint64_t getDependentUnflatDataChunkPos(
    const Expression& logicalRootExpr, PhysicalOperatorsInfo& physicalOperatorInfo);

unique_ptr<PhysicalPlan> PlanMapper::mapToPhysical(
    unique_ptr<LogicalPlan> logicalPlan, ExecutionContext& context) {
    auto physicalOperatorInfo = PhysicalOperatorsInfo();
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalPlan->lastOperator, physicalOperatorInfo, context);
    auto resultCollector = make_unique<ResultCollector>(move(prevOperator), RESULT_COLLECTOR,
        context, physicalOperatorID++,
        logicalPlan->lastOperator->getLogicalOperatorType() == LOGICAL_PROJECTION);
    return make_unique<PhysicalPlan>(move(resultCollector));
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalOperatorToPhysical(
    const shared_ptr<LogicalOperator>& logicalOperator, PhysicalOperatorsInfo& physicalOperatorInfo,
    ExecutionContext& context) {
    unique_ptr<PhysicalOperator> physicalOperator;
    auto operatorType = logicalOperator->getLogicalOperatorType();
    switch (operatorType) {
    case LOGICAL_SCAN_NODE_ID:
        physicalOperator =
            mapLogicalScanNodeIDToPhysical(logicalOperator.get(), physicalOperatorInfo, context);
        break;
    case LOGICAL_EXTEND:
        physicalOperator =
            mapLogicalExtendToPhysical(logicalOperator.get(), physicalOperatorInfo, context);
        break;
    case LOGICAL_FILTER:
        physicalOperator =
            mapLogicalFilterToPhysical(logicalOperator.get(), physicalOperatorInfo, context);
        break;
    case LOGICAL_PROJECTION:
        physicalOperator =
            mapLogicalProjectionToPhysical(logicalOperator.get(), physicalOperatorInfo, context);
        break;
    case LOGICAL_HASH_JOIN:
        physicalOperator =
            mapLogicalHashJoinToPhysical(logicalOperator.get(), physicalOperatorInfo, context);
        break;
    case LOGICAL_SCAN_NODE_PROPERTY:
        physicalOperator = mapLogicalNodePropertyReaderToPhysical(
            logicalOperator.get(), physicalOperatorInfo, context);
        break;
    case LOGICAL_SCAN_REL_PROPERTY:
        physicalOperator = mapLogicalRelPropertyReaderToPhysical(
            logicalOperator.get(), physicalOperatorInfo, context);
        break;
    case LOGICAL_LOAD_CSV:
        physicalOperator =
            mapLogicalLoadCSVToPhysical(logicalOperator.get(), physicalOperatorInfo, context);
        break;
    case LOGICAL_CREATE_NODE:
    case LOGICAL_UPDATE_NODE:
        physicalOperator =
            mapLogicalCRUDNodeToPhysical(logicalOperator.get(), physicalOperatorInfo, context);
        break;
    default:
        assert(false);
    }
    // This map is populated regardless of whether PROFILE or EXPLAIN is enabled.
    physicalIDToLogicalOperatorMap.insert({physicalOperator->id, logicalOperator});
    return physicalOperator;
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanNodeIDToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& physicalOperatorInfo,
    ExecutionContext& context) {
    auto& scan = (const LogicalScanNodeID&)*logicalOperator;
    unique_ptr<PhysicalOperator> prevOperator;
    if (scan.prevOperator) {
        prevOperator =
            mapLogicalOperatorToPhysical(scan.prevOperator, physicalOperatorInfo, context);
    }
    auto morsel = make_shared<MorselsDesc>(scan.label, graph.getNumNodes(scan.label));
    physicalOperatorInfo.appendAsNewDataChunk(scan.nodeID);
    return prevOperator ? make_unique<ScanNodeID<true>>(
                              morsel, move(prevOperator), context, physicalOperatorID++) :
                          make_unique<ScanNodeID<true>>(morsel, context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalExtendToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& physicalOperatorInfo,
    ExecutionContext& context) {
    auto& extend = (const LogicalExtend&)*logicalOperator;
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->prevOperator, physicalOperatorInfo, context);
    auto dataChunkPos = physicalOperatorInfo.getDataChunkPos(extend.boundNodeID);
    auto valueVectorPos = physicalOperatorInfo.getValueVectorPos(extend.boundNodeID);
    auto& relsStore = graph.getRelsStore();
    if (extend.isColumn) {
        assert(extend.lowerBound == extend.upperBound && extend.lowerBound == 1);
        physicalOperatorInfo.appendAsNewValueVector(extend.nbrNodeID, dataChunkPos);
        return make_unique<AdjColumnExtend>(dataChunkPos, valueVectorPos,
            relsStore.getAdjColumn(extend.direction, extend.boundNodeLabel, extend.relLabel),
            move(prevOperator), context, physicalOperatorID++);
    } else {
        if (!physicalOperatorInfo.dataChunkPosToIsFlat[dataChunkPos]) {
            physicalOperatorInfo.dataChunkPosToIsFlat[dataChunkPos] = true;
            prevOperator = make_unique<Flatten>(
                dataChunkPos, move(prevOperator), context, physicalOperatorID++);
        }
        physicalOperatorInfo.appendAsNewDataChunk(extend.nbrNodeID);
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

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalFilterToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& physicalOperatorInfo,
    ExecutionContext& context) {
    auto& logicalFilter = (const LogicalFilter&)*logicalOperator;
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->prevOperator, physicalOperatorInfo, context);
    const auto& logicalRootExpr = logicalFilter.getRootExpression();
    prevOperator = appendFlattenOperatorsIfNecessary(
        logicalRootExpr, move(prevOperator), physicalOperatorInfo, context);
    auto dataChunkToSelectPos =
        getDependentUnflatDataChunkPos(logicalRootExpr, physicalOperatorInfo);
    auto physicalRootExpr = ExpressionMapper::mapToPhysical(*context.memoryManager, logicalRootExpr,
        physicalOperatorInfo, *prevOperator->getResultSet());
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
    PhysicalOperatorsInfo prevOperatorInfo;
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->prevOperator, prevOperatorInfo, context);
    auto& logicalProjection = (const LogicalProjection&)*logicalOperator;
    // We obtain the number of dataChunks from the previous operator as the projection operator
    // creates new dataChunks and we will create a new physicalOperatorInfo object.
    auto numInputDataChunks = prevOperatorInfo.vectorVariables.size();

    // We append flatten(s) as necessary and map each logical expression to a expression_evaluator
    // one.
    vector<unique_ptr<ExpressionEvaluator>> expressionEvaluators;
    for (const auto& logicalRootExpr : logicalProjection.expressionsToProject) {
        prevOperator = appendFlattenOperatorsIfNecessary(
            *logicalRootExpr, move(prevOperator), prevOperatorInfo, context);
        expressionEvaluators.push_back(ExpressionMapper::mapToPhysical(*context.memoryManager,
            *logicalRootExpr, prevOperatorInfo, *prevOperator->getResultSet()));
    }

    // We collect the input data chunk positions for the vectors in the expressions.
    vector<string> expressionName;
    vector<uint64_t> exprResultInDataChunkPos;
    for (const auto& logicalRootExpr : logicalProjection.expressionsToProject) {
        auto name = logicalRootExpr->getInternalName();
        expressionName.push_back(name);
        exprResultInDataChunkPos.push_back(
            prevOperatorInfo.containDataChunk(name) ?
                prevOperatorInfo.getDataChunkPos(name) :
                getDependentUnflatDataChunkPos(*logicalRootExpr, prevOperatorInfo));
    }

    // We map the input data chunk positions from the prev operator to new normalized ones from 0 to
    // m, where m is the number of output data chunk positions.
    vector<uint64_t> exprResultOutDataChunkPos;
    exprResultOutDataChunkPos.resize(expressionEvaluators.size());
    auto currNumOutDataChunks = 0;
    unordered_map<uint64_t, uint64_t> inToOutDataChunkPosMap;
    for (auto i = 0u; i < expressionEvaluators.size(); i++) {
        auto it = inToOutDataChunkPosMap.find(exprResultInDataChunkPos[i]);
        if (it != end(inToOutDataChunkPosMap)) {
            exprResultOutDataChunkPos[i] = inToOutDataChunkPosMap.at(exprResultInDataChunkPos[i]);
            physicalOperatorInfo.appendAsNewValueVector(
                expressionName[i], exprResultOutDataChunkPos[i]);
        } else {
            exprResultOutDataChunkPos[i] = currNumOutDataChunks++;
            inToOutDataChunkPosMap.insert(
                {exprResultInDataChunkPos[i], exprResultOutDataChunkPos[i]});
            auto newDataChunkPos = physicalOperatorInfo.appendAsNewDataChunk(expressionName[i]);
            physicalOperatorInfo.dataChunkPosToIsFlat[newDataChunkPos] =
                prevOperatorInfo.dataChunkPosToIsFlat[exprResultInDataChunkPos[i]];
        }
    }

    // We collect the discarded dataChunk positions in the input data chunks to obtain their
    // multiplicity in the projection operation.
    vector<uint64_t> discardedDataChunkPos;
    for (auto i = 0u; i < numInputDataChunks; i++) {
        if (!inToOutDataChunkPosMap.contains(i)) {
            discardedDataChunkPos.push_back(i);
        }
    }
    return make_unique<Projection>(move(expressionEvaluators), exprResultOutDataChunkPos,
        discardedDataChunkPos, move(prevOperator), context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::appendFlattenOperatorsIfNecessary(
    const Expression& logicalRootExpr, unique_ptr<PhysicalOperator> prevOperator,
    PhysicalOperatorsInfo& physicalOperatorInfo, ExecutionContext& context) {
    pair<unique_ptr<PhysicalOperator>, uint64_t> result;
    set<uint64_t> unFlatDataChunkPosSet;
    for (auto& expression : logicalRootExpr.getIncludedPropertyOrCSVLineExtractExpressions()) {
        auto pos = physicalOperatorInfo.getDataChunkPos(expression->getInternalName());
        if (!physicalOperatorInfo.dataChunkPosToIsFlat[pos]) {
            unFlatDataChunkPosSet.insert(pos);
        }
    }
    if (!unFlatDataChunkPosSet.empty()) {
        vector<uint64_t> unFlatDataChunkPos(
            unFlatDataChunkPosSet.begin(), unFlatDataChunkPosSet.end());
        if (unFlatDataChunkPosSet.size() > 1) {
            for (auto i = 0u; i < unFlatDataChunkPos.size() - 1; i++) {
                auto pos = unFlatDataChunkPos[i];
                prevOperator =
                    make_unique<Flatten>(pos, move(prevOperator), context, physicalOperatorID++);
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
    for (auto& expression : logicalRootExpr.getIncludedPropertyOrCSVLineExtractExpressions()) {
        auto pos = physicalOperatorInfo.getDataChunkPos(expression->getInternalName());
        if (!physicalOperatorInfo.dataChunkPosToIsFlat[pos]) {
            return pos;
        }
    }
    return UINT64_MAX /* null */;
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalNodePropertyReaderToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& physicalOperatorInfo,
    ExecutionContext& context) {
    auto& scanProperty = (const LogicalScanNodeProperty&)*logicalOperator;
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->prevOperator, physicalOperatorInfo, context);
    auto dataChunkPos = physicalOperatorInfo.getDataChunkPos(scanProperty.nodeID);
    auto valueVectorPos = physicalOperatorInfo.getValueVectorPos(scanProperty.nodeID);
    physicalOperatorInfo.appendAsNewValueVector(scanProperty.propertyVariableName, dataChunkPos);
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

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalRelPropertyReaderToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& physicalOperatorInfo,
    ExecutionContext& context) {
    auto& scanProperty = (const LogicalScanRelProperty&)*logicalOperator;
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->prevOperator, physicalOperatorInfo, context);
    auto inDataChunkPos = physicalOperatorInfo.getDataChunkPos(scanProperty.boundNodeID);
    auto inValueVectorPos = physicalOperatorInfo.getValueVectorPos(scanProperty.boundNodeID);
    auto outDataChunkPos = physicalOperatorInfo.getDataChunkPos(scanProperty.nbrNodeID);
    physicalOperatorInfo.appendAsNewValueVector(scanProperty.propertyVariableName, outDataChunkPos);
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
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& physicalOperatorInfo,
    ExecutionContext& context) {
    auto& hashJoin = (const LogicalHashJoin&)*logicalOperator;
    PhysicalOperatorsInfo buildSideOperatorInfo;
    auto probeSidePrevOperator =
        mapLogicalOperatorToPhysical(hashJoin.prevOperator, physicalOperatorInfo, context);
    auto buildSidePrevOperator = mapLogicalOperatorToPhysical(
        hashJoin.buildSidePrevOperator, buildSideOperatorInfo, context);
    auto probeSideKeyDataChunkPos = physicalOperatorInfo.getDataChunkPos(hashJoin.joinNodeID);
    auto probeSideKeyVectorPos = physicalOperatorInfo.getValueVectorPos(hashJoin.joinNodeID);
    auto buildSideKeyDataChunkPos = buildSideOperatorInfo.getDataChunkPos(hashJoin.joinNodeID);
    auto buildSideKeyVectorPos = buildSideOperatorInfo.getValueVectorPos(hashJoin.joinNodeID);
    // Flat probe side key data chunk if necessary
    if (!physicalOperatorInfo.dataChunkPosToIsFlat[probeSideKeyDataChunkPos]) {
        probeSidePrevOperator = make_unique<Flatten>(
            probeSideKeyDataChunkPos, move(probeSidePrevOperator), context, physicalOperatorID++);
        physicalOperatorInfo.dataChunkPosToIsFlat[probeSideKeyDataChunkPos] = true;
    }
    // Build side: 1) append non-key vectors in the key data chunk into probe side key data chunk.
    //             2) append flat non-key data chunks into buildSideNonKeyFlatDataChunk.
    //             3) append unflat non-key data chunks as new data chunks into dataChunks.
    auto& buildSideKeyDataChunk = buildSideOperatorInfo.vectorVariables[buildSideKeyDataChunkPos];
    for (auto i = 0u; i < buildSideKeyDataChunk.size(); i++) {
        if (i == buildSideKeyVectorPos) {
            continue;
        }
        physicalOperatorInfo.appendAsNewValueVector(
            buildSideKeyDataChunk[i], probeSideKeyDataChunkPos);
    }
    bool buildSideHasNonKeyUnFlatDataChunk = false;
    uint64_t buildSideNonKeyFlatDataChunkPos = 0;
    for (auto i = 0u; i < buildSideOperatorInfo.vectorVariables.size(); i++) {
        if (i == buildSideKeyDataChunkPos) {
            continue;
        }
        if (buildSideOperatorInfo.dataChunkPosToIsFlat[i]) {
            if (buildSideNonKeyFlatDataChunkPos == 0) {
                buildSideNonKeyFlatDataChunkPos = physicalOperatorInfo.appendAsNewDataChunk(
                    buildSideOperatorInfo.vectorVariables[i][0]);
            }
            for (auto& variableName : buildSideOperatorInfo.vectorVariables[i]) {
                physicalOperatorInfo.appendAsNewValueVector(
                    variableName, buildSideNonKeyFlatDataChunkPos);
            }
        } else {
            auto newDataChunkPos = physicalOperatorInfo.appendAsNewDataChunk(
                buildSideOperatorInfo.vectorVariables[i][0]);
            for (auto j = 1u; j < buildSideOperatorInfo.vectorVariables[i].size(); j++) {
                physicalOperatorInfo.appendAsNewValueVector(
                    buildSideOperatorInfo.vectorVariables[i][j], newDataChunkPos);
            }
            buildSideHasNonKeyUnFlatDataChunk = true;
        }
    }
    // Set buildSideFlatResultDataChunk as flat if there are unFlat data chunks from the build side.
    if (buildSideHasNonKeyUnFlatDataChunk) {
        physicalOperatorInfo.dataChunkPosToIsFlat[buildSideNonKeyFlatDataChunkPos] = true;
    }
    auto hashJoinBuild = make_unique<HashJoinBuild>(buildSideKeyDataChunkPos, buildSideKeyVectorPos,
        buildSideOperatorInfo.dataChunkPosToIsFlat, move(buildSidePrevOperator), context,
        physicalOperatorID++);
    auto hashJoinSharedState =
        make_shared<HashJoinSharedState>(hashJoinBuild->numBytesForFixedTuplePart);
    hashJoinBuild->sharedState = hashJoinSharedState;
    auto hashJoinProbe = make_unique<HashJoinProbe<true>>(buildSideKeyDataChunkPos,
        buildSideKeyVectorPos, buildSideOperatorInfo.dataChunkPosToIsFlat, probeSideKeyDataChunkPos,
        probeSideKeyVectorPos, move(hashJoinBuild), move(probeSidePrevOperator), context,
        physicalOperatorID++);
    hashJoinProbe->sharedState = hashJoinSharedState;
    return hashJoinProbe;
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalLoadCSVToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& physicalOperatorInfo,
    ExecutionContext& context) {
    auto& logicalLoadCSV = (const LogicalLoadCSV&)*logicalOperator;
    assert(!logicalLoadCSV.csvColumnVariables.empty());
    vector<DataType> csvColumnDataTypes;
    uint64_t dataChunkPos;
    for (auto i = 0u; i < logicalLoadCSV.csvColumnVariables.size(); i++) {
        auto csvColumnName = logicalLoadCSV.csvColumnVariables[i]->getInternalName();
        if (0u == i) {
            dataChunkPos = physicalOperatorInfo.appendAsNewDataChunk(csvColumnName);
        } else {
            physicalOperatorInfo.appendAsNewValueVector(csvColumnName, dataChunkPos);
        }
        csvColumnDataTypes.push_back(logicalLoadCSV.csvColumnVariables[i]->dataType);
    }
    return make_unique<LoadCSV<true>>(logicalLoadCSV.path, logicalLoadCSV.tokenSeparator,
        csvColumnDataTypes, context, physicalOperatorID++);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCRUDNodeToPhysical(
    LogicalOperator* logicalOperator, PhysicalOperatorsInfo& physicalOperatorInfo,
    ExecutionContext& context) {
    auto& logicalCRUDNode = (const LogicalCRUDNode&)*logicalOperator;
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->prevOperator, physicalOperatorInfo, context);
    auto& catalog = graph.getCatalog();
    auto& properties = catalog.getNodeProperties(logicalCRUDNode.nodeLabel);
    vector<BaseColumn*> nodePropertyColumns(properties.size());
    for (auto& entry : properties) {
        if (entry.dataType != UNSTRUCTURED) {
            nodePropertyColumns[entry.id] =
                graph.getNodesStore().getNodePropertyColumn(logicalCRUDNode.nodeLabel, entry.id);
        }
    }
    auto& propertyKeyToColVarMap = logicalCRUDNode.propertyKeyToCSVColumnVariableMap;
    vector<uint32_t> propertyKeyVectorPos(properties.size(), -1);
    uint32_t dataChunkPos;
    for (auto& entry : propertyKeyToColVarMap) {
        propertyKeyVectorPos[entry.first] = physicalOperatorInfo.getValueVectorPos(entry.second);
        dataChunkPos = physicalOperatorInfo.getDataChunkPos(entry.second);
    }
    if (LOGICAL_CREATE_NODE == logicalCRUDNode.getLogicalOperatorType()) {
        return make_unique<CreateNode>(dataChunkPos, move(propertyKeyVectorPos),
            logicalCRUDNode.nodeLabel, move(nodePropertyColumns),
            graph.getNumNodes(logicalCRUDNode.nodeLabel), move(prevOperator), context,
            physicalOperatorID++);
    } else if (LOGICAL_UPDATE_NODE == logicalCRUDNode.getLogicalOperatorType()) {
        return make_unique<UpdateNode>(dataChunkPos, move(propertyKeyVectorPos),
            logicalCRUDNode.nodeLabel, move(nodePropertyColumns),
            graph.getNumNodes(logicalCRUDNode.nodeLabel), move(prevOperator), context,
            physicalOperatorID++);
    } else if (LOGICAL_DELETE_NODE == logicalCRUDNode.getLogicalOperatorType()) {
        return make_unique<DeleteNode>(dataChunkPos, move(propertyKeyVectorPos),
            logicalCRUDNode.nodeLabel, move(nodePropertyColumns),
            graph.getNumNodes(logicalCRUDNode.nodeLabel), move(prevOperator), context,
            physicalOperatorID++);
    }
    assert(false);
}

} // namespace processor
} // namespace graphflow
