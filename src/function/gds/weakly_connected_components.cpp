#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/gds_utils.h"
#include "function/gds_function.h"
#include "gds_vertex_compute.h"
#include "processor/execution_context.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

struct WccComputationState {
    AtomicObjectArraysMap<offset_t> wccIDsMap;

    WccComputationState(const table_id_map_t<offset_t>& maxOffsetMap, MemoryManager* mm) {
        for (const auto& [tableID, maxOffset] : maxOffsetMap) {
            wccIDsMap.allocateArray(tableID, maxOffset, mm);
            wccIDsMap.pinTable(tableID);
            for (auto i = 0u; i < maxOffset; ++i) {
                wccIDsMap.setValueRelaxed(i, i);
            }
        }
    }
};

class WCCAuxiliaryState : public GDSAuxiliaryState {
public:
    explicit WCCAuxiliaryState(WccComputationState& wccComputationState)
        : wccComputationState{wccComputationState} {}

    void beginFrontierCompute(common::table_id_t, common::table_id_t toTableID) override {
        wccComputationState.wccIDsMap.pinTable(toTableID);
    }

private:
    WccComputationState& wccComputationState;
};

struct WCCEdgeCompute : public EdgeCompute {
    WccComputationState& wccComputationState;

    explicit WCCEdgeCompute(WccComputationState& wccComputationState)
        : wccComputationState{wccComputationState} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk,
        bool) override {
        std::vector<nodeID_t> result;
        chunk.forEach([&](auto nbrNodeID, auto) {
            if (wccComputationState.wccIDsMap.getArray()->compare_exchange_strong_max(
                    boundNodeID.offset, nbrNodeID.offset)) {
                result.push_back(nbrNodeID);
            }
        });
        return result;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<WCCEdgeCompute>(wccComputationState);
    }
};

class WCCVertexCompute : public GDSResultVertexCompute {
public:
    WCCVertexCompute(storage::MemoryManager* mm, processor::GDSCallSharedState* sharedState,
        WccComputationState& wccComputationState)
        : GDSResultVertexCompute{mm, sharedState}, wccComputationState{wccComputationState} {
        nodeIDVector = createVector(LogicalType::INTERNAL_ID());
        componentIDVector = createVector(LogicalType::UINT64());
    }

    void beginOnTableInternal(table_id_t tableID) override {
        wccComputationState.wccIDsMap.pinTable(tableID);
    }

    void vertexCompute(common::offset_t startOffset, common::offset_t endOffset,
        common::table_id_t tableID) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            if (skip(i)) {
                continue;
            }
            auto nodeID = nodeID_t{i, tableID};
            nodeIDVector->setValue<nodeID_t>(0, nodeID);
            componentIDVector->setValue<uint64_t>(0,
                wccComputationState.wccIDsMap.getValueRelaxed(i));
            localFT->append(vectors);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<WCCVertexCompute>(mm, sharedState, wccComputationState);
    }

private:
    WccComputationState& wccComputationState;
    std::unique_ptr<ValueVector> nodeIDVector;
    std::unique_ptr<ValueVector> componentIDVector;
};

class WeaklyConnectedComponent final : public GDSAlgorithm {
    static constexpr char GROUP_ID_COLUMN_NAME[] = "group_id";
    static constexpr uint8_t MAX_ITERATION = 100;

public:
    WeaklyConnectedComponent() = default;
    WeaklyConnectedComponent(const WeaklyConnectedComponent& other) : GDSAlgorithm{other} {}

    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return std::vector<LogicalTypeID>{LogicalTypeID::ANY};
    }

    binder::expression_vector getResultColumns(
        const function::GDSBindInput& bindInput) const override {
        expression_vector columns;
        auto& outputNode = bindData->getNodeOutput()->constCast<NodeExpression>();
        columns.push_back(outputNode.getInternalID());
        columns.push_back(
            bindInput.binder->createVariable(GROUP_ID_COLUMN_NAME, LogicalType::INT64()));
        return columns;
    }

    void bind(const GDSBindInput& input, main::ClientContext& context) override {
        auto graphName = binder::ExpressionUtil::getLiteralValue<std::string>(*input.getParam(0));
        auto graphEntry = bindGraphEntry(context, graphName);
        auto nodeOutput = bindNodeOutput(input, graphEntry.getNodeEntries());
        bindData = std::make_unique<GDSBindData>(std::move(graphEntry), nodeOutput);
    }

    void exec(processor::ExecutionContext* context) override {
        auto clientContext = context->clientContext;
        auto graph = sharedState->graph.get();
        auto currentFrontier = PathLengths::getUnvisitedFrontier(context, graph);
        auto nextFrontier =
            PathLengths::getVisitedFrontier(context, graph, sharedState->getGraphNodeMaskMap());
        auto frontierPair =
            std::make_unique<DoublePathLengthsFrontierPair>(currentFrontier, nextFrontier);
        frontierPair->initGDS();
        auto wccComputationState =
            WccComputationState(graph->getMaxOffsetMap(clientContext->getTransaction()),
                clientContext->getMemoryManager());
        auto edgeCompute = std::make_unique<WCCEdgeCompute>(wccComputationState);
        auto vertexCompute = std::make_unique<WCCVertexCompute>(clientContext->getMemoryManager(),
            sharedState.get(), wccComputationState);
        auto auxiliaryState = std::make_unique<WCCAuxiliaryState>(wccComputationState);
        auto computeState = GDSComputeState(std::move(frontierPair), std::move(edgeCompute),
            std::move(auxiliaryState), nullptr);

        GDSUtils::runFrontiersUntilConvergence(context, computeState, graph, ExtendDirection::BOTH,
            MAX_ITERATION);
        GDSUtils::runVertexCompute(context, graph, *vertexCompute);
        sharedState->factorizedTablePool.mergeLocalTables();
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<WeaklyConnectedComponent>(*this);
    }
};

function_set WeaklyConnectedComponentsFunction::getFunctionSet() {
    function_set result;
    auto algo = std::make_unique<WeaklyConnectedComponent>();
    auto function =
        std::make_unique<GDSFunction>(name, algo->getParameterTypeIDs(), std::move(algo));
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
