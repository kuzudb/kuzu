#include "src/processor/include/physical_plan/plan_mapper.h"

#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/filter/logical_filter.h"
#include "src/planner/include/logical_plan/operator/property_reader/logical_node_property_reader.h"
#include "src/planner/include/logical_plan/operator/property_reader/logical_rel_property_reader.h"
#include "src/planner/include/logical_plan/operator/scan/logical_scan.h"
#include "src/processor/include/physical_plan/expression_mapper.h"
#include "src/processor/include/physical_plan/operator/column_reader/adj_column_extend.h"
#include "src/processor/include/physical_plan/operator/column_reader/node_property_column_reader.h"
#include "src/processor/include/physical_plan/operator/column_reader/rel_property_column_reader.h"
#include "src/processor/include/physical_plan/operator/filter/filter.h"
#include "src/processor/include/physical_plan/operator/list_reader/extend/adj_list_flatten_and_extend.h"
#include "src/processor/include/physical_plan/operator/list_reader/extend/adj_list_only_extend.h"
#include "src/processor/include/physical_plan/operator/list_reader/rel_property_list_reader.h"
#include "src/processor/include/physical_plan/operator/scan/physical_scan.h"

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

unique_ptr<PhysicalPlan> PlanMapper::mapToPhysical(
    unique_ptr<LogicalPlan> logicalPlan, const Graph& graph) {
    auto physicalOperatorInfo = PhysicalOperatorsInfo();
    auto sink = make_unique<Sink>(
        mapLogicalOperatorToPhysical(logicalPlan->getLastOperator(), graph, physicalOperatorInfo));
    return make_unique<PhysicalPlan>(move(sink));
}

unique_ptr<PhysicalOperator> mapLogicalOperatorToPhysical(const LogicalOperator& logicalOperator,
    const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto operatorType = logicalOperator.getLogicalOperatorType();
    switch (operatorType) {
    case SCAN:
        return mapLogicalScanToPhysical(logicalOperator, graph, physicalOperatorInfo);
    case EXTEND:
        return mapLogicalExtendToPhysical(logicalOperator, graph, physicalOperatorInfo);
    case FILTER:
        // warning: assumes flattening is to be taken care of by the enumerator and not the mapper.
        //          we do not append any flattening currently.
        return mapLogicalFilterToPhysical(logicalOperator, graph, physicalOperatorInfo);
    case NODE_PROPERTY_READER:
        return mapLogicalNodePropertyReaderToPhysical(logicalOperator, graph, physicalOperatorInfo);
    case REL_PROPERTY_READER:
        return mapLogicalRelPropertyReaderToPhysical(logicalOperator, graph, physicalOperatorInfo);
    default:
        // should never happen.
        throw std::invalid_argument("Unsupported expression type.");
    }
}

unique_ptr<PhysicalOperator> mapLogicalScanToPhysical(const LogicalOperator& logicalOperator,
    const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& scan = (const LogicalScan&)logicalOperator;
    auto morsel = make_shared<MorselDesc>(graph.getNumNodes(scan.label));
    physicalOperatorInfo.put(scan.nodeVarName, 0 /* dataChunkPos */, 0 /* valueVectorPos */);
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
    auto dataChunks = prevOperator->getDataChunks();
    auto& relsStore = graph.getRelsStore();
    if (catalog.isSingleCaridinalityInDir(extend.relLabel, extend.direction)) {
        physicalOperatorInfo.put(
            extend.nbrNodeVarName, dataChunkPos, dataChunks->getNumValueVectors(dataChunkPos));
        return make_unique<AdjColumnExtend>(dataChunkPos, valueVectorPos,
            relsStore.getAdjColumn(extend.direction, extend.boundNodeVarLabel, extend.relLabel),
            move(prevOperator));
    } else {
        auto numChunks = dataChunks->getNumDataChunks();
        physicalOperatorInfo.put(
            extend.nbrNodeVarName, numChunks /*dataChunkPos*/, 0 /*valueVectorPos*/);
        if (physicalOperatorInfo.isFlat(dataChunkPos)) {
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
    auto outValueVectorPos =
        prevOperator->getDataChunks()->getDataChunk(dataChunkPos)->getNumAttributes();
    physicalOperatorInfo.put(propertyReader.nodeVarName + "." + propertyReader.propertyName,
        dataChunkPos, outValueVectorPos);
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
    auto outValueVectorPos =
        prevOperator->getDataChunks()->getDataChunk(outDataChunkPos)->getNumAttributes();
    physicalOperatorInfo.put(propertyReader.relName + "." + propertyReader.propertyName,
        outDataChunkPos, outValueVectorPos);
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

} // namespace processor
} // namespace graphflow
