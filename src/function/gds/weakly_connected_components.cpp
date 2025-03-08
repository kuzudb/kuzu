#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/gds_utils.h"
#include "function/gds/output_writer.h"
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

class ComponentIDs {
public:
    ComponentIDs(const table_id_map_t<offset_t>& maxOffsetMap, MemoryManager* mm) {
        for (const auto& [tableID, maxOffset] : maxOffsetMap) {
            componentIDsMap.allocate(tableID, maxOffset, mm);
            pinTable(tableID);
            for (auto i = 0u; i < maxOffset; ++i) {
                componentIDs[i].store(i, std::memory_order_relaxed);
            }
        }
    }

    void pinTable(table_id_t tableID) { componentIDs = componentIDsMap.getData(tableID); }

    bool update(common::offset_t boundOffset, common::offset_t nbrOffset) {
        auto boundValue = componentIDs[boundOffset].load(std::memory_order_relaxed);
        auto tmp = componentIDs[nbrOffset].load(std::memory_order_relaxed);
        while (tmp > boundValue) {
            if (componentIDs[nbrOffset].compare_exchange_strong(tmp, boundValue)) {
                return true;
            }
        }
        return false;
    }

    common::offset_t getComponentID(offset_t offset) {
        return componentIDs[offset].load(std::memory_order_relaxed);
    }

private:
    std::atomic<common::offset_t>* componentIDs = nullptr;
    ObjectArraysMap<std::atomic<common::offset_t>> componentIDsMap;
};

class WCCAuxiliaryState : public GDSAuxiliaryState {
public:
    explicit WCCAuxiliaryState(ComponentIDs& componentIDs) : componentIDs{componentIDs} {}

    void beginFrontierCompute(common::table_id_t, common::table_id_t toTableID) override {
        componentIDs.pinTable(toTableID);
    }

private:
    ComponentIDs& componentIDs;
};

struct WCCEdgeCompute : public EdgeCompute {
    ComponentIDs& componentIDs;

    explicit WCCEdgeCompute(ComponentIDs& componentIDs) : componentIDs{componentIDs} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk,
        bool) override {
        std::vector<nodeID_t> result;
        chunk.forEach([&](auto nbrNodeID, auto) {
            if (componentIDs.update(boundNodeID.offset, nbrNodeID.offset)) {
                result.push_back(nbrNodeID);
            }
        });
        return result;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<WCCEdgeCompute>(componentIDs);
    }
};

class WCCOutputWriter : public GDSOutputWriter {
public:
    explicit WCCOutputWriter(main::ClientContext* context) : GDSOutputWriter{context} {
        nodeIDVector = createVector(LogicalType::INTERNAL_ID(), context->getMemoryManager());
        componentIDVector = createVector(LogicalType::UINT64(), context->getMemoryManager());
    }

    std::unique_ptr<WCCOutputWriter> copy() const {
        return std::make_unique<WCCOutputWriter>(context);
    }

public:
    std::unique_ptr<ValueVector> nodeIDVector;
    std::unique_ptr<ValueVector> componentIDVector;
};

class WCCVertexCompute : public GDSResultVertexCompute {
public:
    WCCVertexCompute(storage::MemoryManager* mm, processor::GDSCallSharedState* sharedState,
        std::unique_ptr<WCCOutputWriter> writer, ComponentIDs& componentIDs)
        : GDSResultVertexCompute{mm, sharedState}, writer{std::move(writer)},
          componentIDs{componentIDs} {}

    void beginOnTableInternal(table_id_t tableID) override { componentIDs.pinTable(tableID); }

    void vertexCompute(common::offset_t startOffset, common::offset_t endOffset,
        common::table_id_t tableID) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            if (skip(i)) {
                continue;
            }
            auto nodeID = nodeID_t{i, tableID};
            writer->nodeIDVector->setValue<nodeID_t>(0, nodeID);
            writer->componentIDVector->setValue<uint64_t>(0, componentIDs.getComponentID(i));
            localFT->append(writer->vectors);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<WCCVertexCompute>(mm, sharedState, writer->copy(), componentIDs);
    }

private:
    std::unique_ptr<WCCOutputWriter> writer;
    ComponentIDs& componentIDs;
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
        auto componentIDs = ComponentIDs(graph->getMaxOffsetMap(clientContext->getTransaction()),
            clientContext->getMemoryManager());
        auto edgeCompute = std::make_unique<WCCEdgeCompute>(componentIDs);
        auto writer = std::make_unique<WCCOutputWriter>(clientContext);
        auto vertexCompute = std::make_unique<WCCVertexCompute>(clientContext->getMemoryManager(),
            sharedState.get(), std::move(writer), componentIDs);
        auto auxiliaryState = std::make_unique<WCCAuxiliaryState>(componentIDs);
        auto computeState = GDSComputeState(std::move(frontierPair), std::move(edgeCompute),
            std::move(auxiliaryState), sharedState->getOutputNodeMaskMap());

        GDSUtils::runFrontiersUntilConvergence(context, computeState, graph, ExtendDirection::BOTH,
            MAX_ITERATION);
        GDSUtils::runVertexCompute(context, graph, *vertexCompute);
        sharedState->mergeLocalTables();
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
