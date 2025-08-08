#include "binder/binder.h"
#include "common/task_system/progress_bar.h"
#include "function/algo_function.h"
#include "function/component_ids.h"
#include "function/config/connected_components_config.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_utils.h"
#include "function/gds/gds_vertex_compute.h"
#include "processor/execution_context.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::graph;
using namespace kuzu::function;

namespace kuzu {
namespace algo_extension {

using Colors = ComponentIDs;
using ColorsPair = ComponentIDsPair;

// Sets the sccId for vertices whose forward and backward colors are the same.
// Also check if there is still different color.
class UpdateComponentIDVertexCompute : public GDSVertexCompute {
public:
    UpdateComponentIDVertexCompute(NodeOffsetMaskMap* nodeMask, ComponentIDs& componentIDs,
        Colors& fwdColors, Colors& bwdColors, std::atomic<bool>& hasDifferentColor)
        : GDSVertexCompute{nodeMask}, componentIDs{componentIDs}, fwdColors{fwdColors},
          bwdColors{bwdColors}, hasDifferentColor{hasDifferentColor} {}

    void beginOnTableInternal(table_id_t tableID) override {
        componentIDs.pinTableID(tableID);
        fwdColors.pinTableID(tableID);
        bwdColors.pinTableID(tableID);
    }

    void vertexCompute(offset_t startOffset, offset_t endOffset, table_id_t) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            if (skip(i)) {
                continue;
            }
            if (componentIDs.hasValidComponentID(i)) {
                continue;
            }
            auto fwdColor = fwdColors.getComponentID(i);
            auto bwdColor = bwdColors.getComponentID(i);
            if (fwdColor == bwdColor) {
                componentIDs.setComponentID(i, fwdColor);
            } else {
                hasDifferentColor.store(true, std::memory_order_relaxed);
            }
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<UpdateComponentIDVertexCompute>(nodeMask, componentIDs, fwdColors,
            bwdColors, hasDifferentColor);
    }

private:
    ComponentIDs& componentIDs;
    Colors& fwdColors;
    Colors& bwdColors;
    std::atomic<bool>& hasDifferentColor;
};

class InitFwdColoringVertexCompute : public GDSVertexCompute {
public:
    InitFwdColoringVertexCompute(NodeOffsetMaskMap* nodeMask, DenseFrontierPair& frontierPair,
        ComponentIDs& componentIDs)
        : GDSVertexCompute{nodeMask}, frontierPair{frontierPair}, componentIDs{componentIDs} {}

    void beginOnTableInternal(table_id_t tableID) override {
        componentIDs.pinTableID(tableID);
        frontierPair.pinNextFrontier(tableID);
    }

    void vertexCompute(offset_t startOffset, offset_t endOffset, table_id_t) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            if (skip(i)) {
                continue;
            }
            // If the SCC ID has already been computed, the node should not be activated.
            if (componentIDs.hasValidComponentID(i)) {
                continue;
            }
            frontierPair.addNodeToNextFrontier(i);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<InitFwdColoringVertexCompute>(nodeMask, frontierPair, componentIDs);
    }

private:
    DenseFrontierPair& frontierPair;
    ComponentIDs& componentIDs;
};

class InitBwdColoringVertexCompute : public GDSVertexCompute {
public:
    InitBwdColoringVertexCompute(NodeOffsetMaskMap* nodeMask, DenseFrontierPair& frontierPair,
        ComponentIDs& componentIDs, Colors& fwdColors, OffsetManager& offsetManager)
        : GDSVertexCompute{nodeMask}, frontierPair{frontierPair}, componentIDs{componentIDs},
          fwdColors{fwdColors}, offsetManager{offsetManager} {}

    void beginOnTableInternal(table_id_t tableID) override {
        componentIDs.pinTableID(tableID);
        fwdColors.pinTableID(tableID);
        frontierPair.pinNextFrontier(tableID);
        offsetManager.pinTableID(tableID);
    }

    void vertexCompute(offset_t startOffset, offset_t endOffset, table_id_t) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            if (skip(i)) {
                continue;
            }
            // If the SCC ID has already been computed, the node should not be activated.
            if (componentIDs.hasValidComponentID(i)) {
                continue;
            }
            // Only pick the "root" vertex (vertex & component have the same id) of fwd colors
            // to do backward coloring
            if (fwdColors.getComponentID(i) == offsetManager.getCurrentOffset() + i) {
                frontierPair.addNodeToNextFrontier(i);
            }
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<InitBwdColoringVertexCompute>(nodeMask, frontierPair, componentIDs,
            fwdColors, offsetManager);
    }

private:
    DenseFrontierPair& frontierPair;
    ComponentIDs& componentIDs;
    Colors& fwdColors;
    OffsetManager& offsetManager;
};

class PropagateIDAuxiliaryState : public GDSAuxiliaryState {
public:
    PropagateIDAuxiliaryState(ComponentIDs& componentIDs, ColorsPair& colorsPair)
        : componentIDs{componentIDs}, colorsPair{colorsPair} {}

    void beginFrontierCompute(table_id_t fromTableID, table_id_t toTableID) override {
        componentIDs.pinTableID(toTableID);
        colorsPair.pinCurTableID(fromTableID);
        colorsPair.pinNextTableID(toTableID);
    }

    void switchToDense(ExecutionContext*, Graph*) override {}

private:
    ComponentIDs& componentIDs;
    ColorsPair& colorsPair;
};

class PropagateIDEdgeCompute : public EdgeCompute {
public:
    explicit PropagateIDEdgeCompute(ComponentIDs& componentIDs, ColorsPair& colorsPair)
        : componentIDs{componentIDs}, colorsPair{colorsPair} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk,
        bool) override {
        std::vector<nodeID_t> result;
        chunk.forEach([&](auto neighbors, auto, auto i) {
            auto nbrNodeID = neighbors[i];
            if (!componentIDs.hasValidComponentID(nbrNodeID.offset) &&
                colorsPair.update(boundNodeID.offset, nbrNodeID.offset)) {
                result.push_back(nbrNodeID);
            }
        });
        return result;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<PropagateIDEdgeCompute>(componentIDs, colorsPair);
    }

private:
    ComponentIDs& componentIDs;
    ColorsPair& colorsPair;
};

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
    auto clientContext = input.context->clientContext;
    auto sharedState = input.sharedState->ptrCast<GDSFuncSharedState>();
    auto nodeMask = sharedState->getGraphNodeMaskMap();
    auto mm = clientContext->getMemoryManager();
    auto graph = sharedState->graph.get();
    auto maxOffsetMap = graph->getMaxOffsetMap(clientContext->getTransaction());
    auto maxIterations = input.bindData->optionalParams->constCast<MaxIterationOptionalParams>()
                             .maxIterations.getParamVal();
    auto currentFrontier = DenseFrontier::getUnvisitedFrontier(input.context, graph);
    auto nextFrontier = DenseFrontier::getUnvisitedFrontier(input.context, graph);
    auto frontierPair =
        std::make_shared<DenseFrontierPair>(std::move(currentFrontier), std::move(nextFrontier));

    auto componentIDs = ComponentIDs::getUnvisitedComponentIDs(maxOffsetMap, mm);
    auto offsetManager = OffsetManager(maxOffsetMap);
    auto fwdColors = ComponentIDs::getSequenceComponentIDs(maxOffsetMap, offsetManager, mm);
    auto bwdColors = ComponentIDs::getSequenceComponentIDs(maxOffsetMap, offsetManager, mm);
    auto fwdColorsPair = ComponentIDsPair(fwdColors);
    auto bwdColorsPair = ComponentIDsPair(bwdColors);

    auto initFwdComponentIDsVertexCompute =
        InitSequenceComponentIDsVertexCompute(fwdColors, offsetManager);
    auto initBwdComponentIDsVertexCompute =
        InitSequenceComponentIDsVertexCompute(bwdColors, offsetManager);

    auto initFwdColoringVertexCompute =
        InitFwdColoringVertexCompute(nodeMask, *frontierPair, componentIDs);
    auto fwdColoringEdgeCompute =
        std::make_unique<PropagateIDEdgeCompute>(componentIDs, fwdColorsPair);
    auto fwdAuxiliaryState =
        std::make_unique<PropagateIDAuxiliaryState>(componentIDs, fwdColorsPair);
    auto fwdComputeState = GDSComputeState(frontierPair, std::move(fwdColoringEdgeCompute),
        std::move(fwdAuxiliaryState));

    auto initBwdColoringVertexCompute = InitBwdColoringVertexCompute(nodeMask, *frontierPair,
        componentIDs, fwdColors, offsetManager);
    auto bwdColoringEdgeCompute =
        std::make_unique<PropagateIDEdgeCompute>(componentIDs, bwdColorsPair);
    auto bwdAuxiliaryState =
        std::make_unique<PropagateIDAuxiliaryState>(componentIDs, bwdColorsPair);
    auto bwdComputeState = GDSComputeState(frontierPair, std::move(bwdColoringEdgeCompute),
        std::move(bwdAuxiliaryState));

    auto progressBar = clientContext->getProgressBar();
    for (auto i = 0u; i < maxIterations; i++) {
        // Init fwd and bwd component IDs to node offsets.
        GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, graph,
            initFwdComponentIDsVertexCompute);
        GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, graph,
            initBwdComponentIDsVertexCompute);

        // Fwd colors.
        frontierPair->resetValue(input.context, graph, FRONTIER_UNVISITED);
        fwdComputeState.frontierPair->resetCurrentIter();
        fwdComputeState.frontierPair->setActiveNodesForNextIter();
        GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, graph,
            initFwdColoringVertexCompute);
        GDSUtils::runAlgorithmEdgeCompute(input.context, fwdComputeState, graph,
            ExtendDirection::FWD, maxIterations);
        double progress = static_cast<double>(i) / maxIterations;
        progressBar->updateProgress(input.context->queryID, progress * 0.5);

        // Bwd colors.
        frontierPair->resetValue(input.context, graph, FRONTIER_UNVISITED);
        bwdComputeState.frontierPair->resetCurrentIter();
        bwdComputeState.frontierPair->setActiveNodesForNextIter();
        GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, graph,
            initBwdColoringVertexCompute);
        GDSUtils::runAlgorithmEdgeCompute(input.context, bwdComputeState, graph,
            ExtendDirection::BWD, maxIterations);
        progressBar->updateProgress(input.context->queryID, progress);

        // Find new SCC IDs and exit if all IDs have been found.
        std::atomic<bool> hasDifferentColor = false;
        auto updateComponentIDsVertexCompute = UpdateComponentIDVertexCompute(nodeMask,
            componentIDs, fwdColors, bwdColors, hasDifferentColor);
        GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, graph,
            updateComponentIDsVertexCompute);
        if (!hasDifferentColor) {
            break;
        }
    }

    auto outputVertexCompute =
        std::make_unique<ComponentIDsOutputVertexCompute>(mm, sharedState, componentIDs);
    GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, graph, *outputVertexCompute);

    sharedState->factorizedTablePool.mergeLocalTables();
    return 0;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto graphName = input->getLiteralVal<std::string>(0);
    auto graphEntry = GDSFunction::bindGraphEntry(*context, graphName);
    auto nodeOutput = GDSFunction::bindNodeOutput(*input, graphEntry.getNodeEntries());
    expression_vector columns;
    columns.push_back(nodeOutput->constCast<NodeExpression>().getInternalID());
    columns.push_back(input->binder->createVariable(GROUP_ID_COLUMN_NAME, LogicalType::INT64()));
    auto bindData =
        std::make_unique<GDSBindData>(std::move(columns), std::move(graphEntry), nodeOutput);
    bindData->optionalParams =
        std::make_unique<MaxIterationOptionalParams>(input->optionalParamsLegacy);
    return bindData;
}

function_set SCCFunction::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<TableFunction>(SCCFunction::name,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY});
    func->bindFunc = bindFunc;
    func->tableFunc = tableFunc;
    func->initSharedStateFunc = GDSFunction::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = [] { return false; };
    func->getLogicalPlanFunc = GDSFunction::getLogicalPlan;
    func->getPhysicalPlanFunc = GDSFunction::getPhysicalPlan;
    result.push_back(std::move(func));
    return result;
}

} // namespace algo_extension
} // namespace kuzu
