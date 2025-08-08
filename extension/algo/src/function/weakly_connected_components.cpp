#include "binder/binder.h"
#include "function/algo_function.h"
#include "function/component_ids.h"
#include "function/config/connected_components_config.h"
#include "function/gds/gds_utils.h"
#include "processor/execution_context.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::graph;
using namespace kuzu::function;

namespace kuzu {
namespace algo_extension {

class WCCAuxiliaryState : public GDSAuxiliaryState {
public:
    explicit WCCAuxiliaryState(ComponentIDsPair& componentIDsPair)
        : componentIDsPair{componentIDsPair} {}

    void beginFrontierCompute(table_id_t fromTableID, table_id_t toTableID) override {
        componentIDsPair.pinCurTableID(fromTableID);
        componentIDsPair.pinNextTableID(toTableID);
    }

    void switchToDense(ExecutionContext*, Graph*) override {}

private:
    ComponentIDsPair& componentIDsPair;
};

class WCCEdgeCompute : public EdgeCompute {
public:
    explicit WCCEdgeCompute(ComponentIDsPair& componentIDsPair)
        : componentIDsPair{componentIDsPair} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, NbrScanState::Chunk& chunk,
        bool) override {
        std::vector<nodeID_t> result;
        chunk.forEach([&](auto neighbors, auto, auto i) {
            auto nbrNodeID = neighbors[i];
            if (componentIDsPair.update(boundNodeID.offset, nbrNodeID.offset)) {
                result.push_back(nbrNodeID);
            }
        });
        return result;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<WCCEdgeCompute>(componentIDsPair);
    }

private:
    ComponentIDsPair& componentIDsPair;
};

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
    auto clientContext = input.context->clientContext;
    auto sharedState = input.sharedState->ptrCast<GDSFuncSharedState>();
    auto graph = sharedState->graph.get();
    auto currentFrontier = DenseFrontier::getUnvisitedFrontier(input.context, graph);
    auto nextFrontier =
        DenseFrontier::getVisitedFrontier(input.context, graph, sharedState->getGraphNodeMaskMap());
    auto frontierPair =
        std::make_unique<DenseFrontierPair>(std::move(currentFrontier), std::move(nextFrontier));
    frontierPair->setActiveNodesForNextIter();
    auto maxOffsetMap = graph->getMaxOffsetMap(clientContext->getTransaction());
    auto offsetManager = OffsetManager(maxOffsetMap);
    auto componentIDs = ComponentIDs::getSequenceComponentIDs(maxOffsetMap, offsetManager,
        clientContext->getMemoryManager());
    auto componentIDsPair = ComponentIDsPair(componentIDs);
    auto auxiliaryState = std::make_unique<WCCAuxiliaryState>(componentIDsPair);
    auto edgeCompute = std::make_unique<WCCEdgeCompute>(componentIDsPair);
    auto vertexCompute = std::make_unique<ComponentIDsOutputVertexCompute>(
        clientContext->getMemoryManager(), sharedState, componentIDs);
    auto computeState =
        GDSComputeState(std::move(frontierPair), std::move(edgeCompute), std::move(auxiliaryState));
    auto maxIterations = input.bindData->optionalParams->constCast<MaxIterationOptionalParams>()
                             .maxIterations.getParamVal();
    GDSUtils::runAlgorithmEdgeCompute(input.context, computeState, graph, ExtendDirection::BOTH,
        maxIterations);
    GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, graph, *vertexCompute);
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

function_set WeaklyConnectedComponentsFunction::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<TableFunction>(name, std::vector{LogicalTypeID::ANY});
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
