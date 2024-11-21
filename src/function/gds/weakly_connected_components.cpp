#include "binder/binder.h"
#include "common/types/types.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/gds_utils.h"
#include "function/gds/rec_joins.h"
#include "function/gds_function.h"
#include "graph/graph.h"
#include "processor/execution_context.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

struct WCCEdgeCompute : public EdgeCompute {
    WCCFrontierPair* frontierPair;

    explicit WCCEdgeCompute(WCCFrontierPair* frontierPair) : frontierPair{frontierPair} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::GraphScanState::Chunk& chunk,
        bool) override {
        std::vector<nodeID_t> result;
        chunk.forEach([&](auto nbrNodeID, auto) {
            if (frontierPair->update(boundNodeID, nbrNodeID)) {
                result.push_back(nbrNodeID);
            }
        });
        return result;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<WCCEdgeCompute>(frontierPair);
    }
};

class WCCOutputWriter {
public:
    explicit WCCOutputWriter(storage::MemoryManager* mm) {
        nodeIDVector = getVector(LogicalType::INTERNAL_ID(), mm);
        componentIDVector = getVector(LogicalType::UINT64(), mm);
    }

    void materialize(nodeID_t nodeID, uint64_t componentID, FactorizedTable& table) const {
        nodeIDVector->setValue<nodeID_t>(0, nodeID);
        componentIDVector->setValue<uint64_t>(0, componentID);
        table.append(vectors);
    }

private:
    std::unique_ptr<ValueVector> getVector(const LogicalType& type, storage::MemoryManager* mm) {
        auto vector = std::make_unique<ValueVector>(type.copy(), mm);
        vector->state = DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(vector.get());
        return vector;
    }

private:
    std::unique_ptr<ValueVector> nodeIDVector;
    std::unique_ptr<ValueVector> componentIDVector;
    std::vector<ValueVector*> vectors;
};

class WCCVertexCompute : public VertexCompute {
public:
    WCCVertexCompute(storage::MemoryManager* mm, processor::GDSCallSharedState* sharedState,
        const RJCompState& compState)
        : mm{mm}, sharedState{sharedState}, compState{compState}, outputWriter{mm} {
        localFT = sharedState->claimLocalTable(mm);
    }
    ~WCCVertexCompute() override { sharedState->returnLocalTable(localFT); }

    bool beginOnTable(common::table_id_t) override { return true; }

    void vertexCompute(common::nodeID_t curNodeID) override {
        const auto& frontierPair = compState.frontierPair->ptrCast<WCCFrontierPair>();
        auto id = frontierPair->getComponentID(curNodeID);
        outputWriter.materialize(curNodeID, id, *localFT);
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<WCCVertexCompute>(mm, sharedState, compState);
    }

private:
    storage::MemoryManager* mm;
    processor::GDSCallSharedState* sharedState;
    const RJCompState& compState;
    WCCOutputWriter outputWriter;
    processor::FactorizedTable* localFT;
};

class WeaklyConnectedComponent final : public GDSAlgorithm {
    static constexpr char GROUP_ID_COLUMN_NAME[] = "group_id";

public:
    WeaklyConnectedComponent() = default;
    WeaklyConnectedComponent(const WeaklyConnectedComponent& other) : GDSAlgorithm{other} {}

    /*
     * Inputs are
     *
     * graph::ANY
     */
    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return std::vector<LogicalTypeID>{LogicalTypeID::ANY};
    }

    /*
     * Outputs are
     *
     * _node._id::INTERNAL_ID
     * group_id::INT64
     */
    binder::expression_vector getResultColumns(binder::Binder* binder) const override {
        expression_vector columns;
        auto& outputNode = bindData->getNodeOutput()->constCast<NodeExpression>();
        columns.push_back(outputNode.getInternalID());
        columns.push_back(binder->createVariable(GROUP_ID_COLUMN_NAME, LogicalType::INT64()));
        return columns;
    }

    void bind(const expression_vector&, Binder* binder, GraphEntry& graphEntry) override {
        auto nodeOutput = bindNodeOutput(binder, graphEntry);
        bindData = std::make_unique<GDSBindData>(nodeOutput);
    }

    void exec(processor::ExecutionContext* context) override {
        auto clientContext = context->clientContext;
        auto graph = sharedState->graph.get();
        auto nodeTableIDs = graph->getNodeTableIDs();
        uint64_t totalNumNodes = 0;
        for (auto& nodeTableID : nodeTableIDs) {
            totalNumNodes += graph->getNumNodes(clientContext->getTx(), nodeTableID);
        }
        auto frontierPair = std::make_unique<WCCFrontierPair>(graph->getNumNodesMap(clientContext->getTx()), totalNumNodes,
            clientContext->getMaxNumThreadForExec(), clientContext->getMemoryManager());
        auto edgeCompute = std::make_unique<WCCEdgeCompute>(frontierPair.get());
        // GDS::Utils::RunUntilConvergence shouldn't explicitly call RJCompState since other
        // algorithms can also use RunUntilConvergence. A solution could be to have a general
        // CompState class which RJCompState derives from.
        auto computeState =
            RJCompState(std::move(frontierPair), std::move(edgeCompute), nullptr, nullptr);
        GDSUtils::runFrontiersUntilConvergence(context, computeState, graph, ExtendDirection::FWD,
            10);
        auto vertexCompute = std::make_unique<WCCVertexCompute>(clientContext->getMemoryManager(),
            sharedState.get(), computeState);
        GDSUtils::runVertexComputeIteration(context, sharedState->graph.get(), *vertexCompute);
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
