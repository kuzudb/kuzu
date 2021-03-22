#include "src/processor/include/physical_plan/plan_mapper.h"

#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/property_reader/logical_node_property_reader.h"
#include "src/planner/include/logical_plan/operator/property_reader/logical_rel_property_reader.h"
#include "src/planner/include/logical_plan/operator/scan/logical_scan.h"
#include "src/processor/include/physical_plan/operator/column_reader/adj_column_extend.h"
#include "src/processor/include/physical_plan/operator/column_reader/node_property_column_reader.h"
#include "src/processor/include/physical_plan/operator/column_reader/rel_property_column_reader.h"
#include "src/processor/include/physical_plan/operator/list_reader/extend/adj_list_flatten_and_extend.h"
#include "src/processor/include/physical_plan/operator/list_reader/extend/adj_list_only_extend.h"
#include "src/processor/include/physical_plan/operator/list_reader/rel_property_list_reader.h"
#include "src/processor/include/physical_plan/operator/scan/physical_scan.h"

using namespace graphflow::planner;

namespace graphflow {
namespace processor {

static unique_ptr<PhysicalOperator> mapLogicalOperatorToPhysical(
    unique_ptr<LogicalOperator> logicalOperator, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo);

static unique_ptr<PhysicalOperator> mapLogicalScanToPhysical(
    unique_ptr<LogicalOperator> logicalOperator, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo);

static unique_ptr<PhysicalOperator> mapLogicalExtendToPhysical(
    unique_ptr<LogicalOperator> logicalOperator, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo);

static unique_ptr<PhysicalOperator> mapLogicalNodePropertyReaderToPhysical(
    unique_ptr<LogicalOperator> logicalOperator, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo);

static unique_ptr<PhysicalOperator> mapLogicalRelPropertyReaderToPhysical(
    unique_ptr<LogicalOperator> logicalOperator, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo);

unique_ptr<PhysicalPlan> PlanMapper::mapToPhysical(
    unique_ptr<LogicalPlan> logicalPlan, const Graph& graph) {
    auto physicalOperatorInfo = PhysicalOperatorsInfo();
    auto lastLogicalOperator = logicalPlan->getLastOperator();
    auto sink = make_unique<Sink>(
        mapLogicalOperatorToPhysical(move(lastLogicalOperator), graph, physicalOperatorInfo));
    return make_unique<PhysicalPlan>(move(sink));
}

unique_ptr<PhysicalOperator> mapLogicalOperatorToPhysical(
    unique_ptr<LogicalOperator> logicalOperator, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto operatorType = logicalOperator->getLogicalOperatorType();
    switch (operatorType) {
    case SCAN:
        return mapLogicalScanToPhysical(move(logicalOperator), graph, physicalOperatorInfo);
    case EXTEND:
        return mapLogicalExtendToPhysical(move(logicalOperator), graph, physicalOperatorInfo);
    case NODE_PROPERTY_READER:
        return mapLogicalNodePropertyReaderToPhysical(
            move(logicalOperator), graph, physicalOperatorInfo);
    case REL_PROPERTY_READER:
        return mapLogicalRelPropertyReaderToPhysical(
            move(logicalOperator), graph, physicalOperatorInfo);
    default:
        // should never happen.
        throw std::invalid_argument("Unsupported expression type.");
    }
}

unique_ptr<PhysicalOperator> mapLogicalScanToPhysical(unique_ptr<LogicalOperator> logicalOperator,
    const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& scan = *(static_cast<LogicalScan*>(logicalOperator.get()));
    auto& catalog = graph.getCatalog();
    auto labelFromStr = catalog.getNodeLabelFromString(scan.label.c_str());
    auto morsel = make_shared<MorselDesc>(graph.getNumNodes(labelFromStr));
    physicalOperatorInfo.put(scan.nodeVarName, 0 /* dataChunkPos */, 0 /* valueVectorPos */);
    return make_unique<PhysicalScan>(morsel);
}

unique_ptr<PhysicalOperator> mapLogicalExtendToPhysical(unique_ptr<LogicalOperator> logicalOperator,
    const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& extend = *(static_cast<LogicalExtend*>(logicalOperator.get()));
    auto prevOperator = mapLogicalOperatorToPhysical(
        move(logicalOperator->prevOperator), graph, physicalOperatorInfo);
    auto dataChunkPos = physicalOperatorInfo.getDataChunkPos(extend.boundNodeVarName);
    auto valueVectorPos = physicalOperatorInfo.getValueVectorPos(extend.boundNodeVarName);
    auto& catalog = graph.getCatalog();
    auto relLabelFromString = catalog.getRelLabelFromString(extend.relLabel.c_str());
    auto dataChunks = prevOperator->getDataChunks();
    auto nodeLabel = catalog.getNodeLabelFromString(extend.boundNodeVarLabel.c_str());
    auto& relsStore = graph.getRelsStore();
    if (catalog.isSingleCaridinalityInDir(relLabelFromString, extend.direction)) {
        physicalOperatorInfo.put(
            extend.nbrNodeVarName, dataChunkPos, dataChunks->getNumValueVectors(dataChunkPos));
        return make_unique<AdjColumnExtend>(dataChunkPos, valueVectorPos,
            relsStore.getAdjColumn(extend.direction, nodeLabel, relLabelFromString),
            move(prevOperator));
    } else {
        auto numChunks = dataChunks->getNumDataChunks();
        physicalOperatorInfo.put(
            extend.nbrNodeVarName, numChunks /*dataChunkPos*/, 0 /*valueVectorPos*/);
        if (physicalOperatorInfo.isFlat(dataChunkPos)) {
            return make_unique<AdjListOnlyExtend>(dataChunkPos, valueVectorPos,
                relsStore.getAdjLists(extend.direction, nodeLabel, relLabelFromString),
                move(prevOperator));
        } else {
            physicalOperatorInfo.setDataChunkAtPosAsFlat(dataChunkPos);
            return make_unique<AdjListFlattenAndExtend>(dataChunkPos, valueVectorPos,
                relsStore.getAdjLists(extend.direction, nodeLabel, relLabelFromString),
                move(prevOperator));
        }
    }
}

unique_ptr<PhysicalOperator> mapLogicalNodePropertyReaderToPhysical(
    unique_ptr<LogicalOperator> logicalOperator, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& properyReader = *(static_cast<LogicalNodePropertyReader*>(logicalOperator.get()));
    auto prevOperator = mapLogicalOperatorToPhysical(
        move(logicalOperator->prevOperator), graph, physicalOperatorInfo);
    auto dataChunkPos = physicalOperatorInfo.getDataChunkPos(properyReader.nodeVarName);
    auto valueVectorPos = physicalOperatorInfo.getValueVectorPos(properyReader.nodeVarName);
    auto& catalog = graph.getCatalog();
    auto label = catalog.getNodeLabelFromString(properyReader.nodeLabel.c_str());
    auto property = catalog.getNodePropertyKeyFromString(label, properyReader.propertyName);
    auto& nodesStore = graph.getNodesStore();
    return make_unique<NodePropertyColumnReader>(dataChunkPos, valueVectorPos,
        nodesStore.getNodePropertyColumn(label, property), move(prevOperator));
}

unique_ptr<PhysicalOperator> mapLogicalRelPropertyReaderToPhysical(
    unique_ptr<LogicalOperator> logicalOperator, const Graph& graph,
    PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto& properyReader = *(static_cast<LogicalRelPropertyReader*>(logicalOperator.get()));
    auto prevOperator = mapLogicalOperatorToPhysical(
        move(logicalOperator->prevOperator), graph, physicalOperatorInfo);
    auto& catalog = graph.getCatalog();
    auto label = catalog.getRelLabelFromString(properyReader.relLabel.c_str());
    auto inDataChunkPos = physicalOperatorInfo.getDataChunkPos(properyReader.boundNodeVarName);
    auto inValueVectorPos = physicalOperatorInfo.getValueVectorPos(properyReader.boundNodeVarName);
    auto outDataChunkPos = physicalOperatorInfo.getDataChunkPos(properyReader.nbrNodeVarName);
    auto nodeLabel = catalog.getNodeLabelFromString(properyReader.boundNodeVarLabel.c_str());
    auto property = catalog.getRelPropertyKeyFromString(label, properyReader.propertyName);
    auto& relsStore = graph.getRelsStore();
    if (catalog.isSingleCaridinalityInDir(label, properyReader.direction)) {
        auto column = relsStore.getRelPropertyColumn(label, nodeLabel, property);
        return make_unique<RelPropertyColumnReader>(
            inDataChunkPos, inValueVectorPos, column, move(prevOperator));
    } else {
        auto lists =
            relsStore.getRelPropertyLists(properyReader.direction, nodeLabel, label, property);
        return make_unique<RelPropertyListReader>(
            inDataChunkPos, inValueVectorPos, outDataChunkPos, lists, move(prevOperator));
    }
}

} // namespace processor
} // namespace graphflow
