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
struct KCoreInitEdgeCompute : public EdgeCompute {
    KCoreFrontierPair* frontierPair;

    explicit KCoreInitEdgeCompute(KCoreFrontierPair* frontierPair) : frontierPair{frontierPair} {}

    std::vector<common::nodeID_t> edgeCompute(common::nodeID_t boundNodeID,
        graph::NbrScanState::Chunk& chunk, bool) override {
        std::vector<common::nodeID_t> result;
        uint64_t nbrAmount = 0;
        chunk.forEach([&](auto nbrNodeID, auto) {
            nbrAmount++;
            result.push_back(nbrNodeID);
        });
        frontierPair->addToVertexDegree(boundNodeID, nbrAmount);
        return result;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<KCoreInitEdgeCompute>(frontierPair);
    }
};

struct KCoreEdgeCompute : public EdgeCompute {
    KCoreFrontierPair* frontierPair;

    explicit KCoreEdgeCompute(KCoreFrontierPair* frontierPair) : frontierPair{frontierPair} {}

    std::vector<common::nodeID_t> edgeCompute(common::nodeID_t boundNodeID,
        graph::NbrScanState::Chunk& chunk, bool) override {
        std::vector<common::nodeID_t> result;
        uint64_t vertexDegree = frontierPair->getVertexDegree(boundNodeID);
        uint64_t smallestDegree = frontierPair->getSmallestDegree();
        if (frontierPair->isNodeActive(boundNodeID) && (vertexDegree == smallestDegree)) {
            frontierPair->setNodeInActive(boundNodeID);
            chunk.forEach([&](auto nbrNodeID, auto) {
                if (frontierPair->isNodeActive(nbrNodeID)) {
                    frontierPair->removeFromVertex(nbrNodeID);
                    result.push_back(nbrNodeID);
                }
            });
        }
        return result;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<KCoreEdgeCompute>(frontierPair);
    }
};

class KCoreOutputWriter {
public:
    explicit KCoreOutputWriter(storage::MemoryManager* mm) {
        nodeIDVector = getVector(LogicalType::INTERNAL_ID(), mm);
        kValueVector = getVector(LogicalType::UINT64(), mm);
    }

    void materialize(nodeID_t nodeID, uint64_t kValue, FactorizedTable& table) const {
        nodeIDVector->setValue<nodeID_t>(0, nodeID);
        kValueVector->setValue<uint64_t>(0, kValue);
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
    std::unique_ptr<ValueVector> kValueVector;
    std::vector<ValueVector*> vectors;
};

class KCoreVertexCompute : public VertexCompute {
public:
    KCoreVertexCompute(storage::MemoryManager* mm, processor::GDSCallSharedState* sharedState,
        const RJCompState& compState)
        : mm{mm}, sharedState{sharedState}, compState{compState}, outputWriter{mm} {
        localFT = sharedState->claimLocalTable(mm);
    }
    ~KCoreVertexCompute() override { sharedState->returnLocalTable(localFT); }

    bool beginOnTable(common::table_id_t) override { return true; }

    void vertexCompute(const graph::VertexScanState::Chunk& chunk) override {
        // const auto& frontierPair = compState.frontierPair->ptrCast<KCoreFrontierPair>();
        // auto id = frontierPair->getComponentID(curNodeID);
        // outputWriter.materialize(curNodeID, id, *localFT);

        for (auto nodeID : chunk.getNodeIDs()) {
            auto frontierPair = compState.frontierPair->ptrCast<KCoreFrontierPair>();
            outputWriter.materialize(nodeID, frontierPair->getVertexDegree(nodeID), *localFT);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<KCoreVertexCompute>(mm, sharedState, compState);
    }

private:
    storage::MemoryManager* mm;
    processor::GDSCallSharedState* sharedState;
    const RJCompState& compState;
    KCoreOutputWriter outputWriter;
    processor::FactorizedTable* localFT;
};

class KCoreDecomposition final : public GDSAlgorithm {
    static constexpr char GROUP_ID_COLUMN_NAME[] = "group_id";

public:
    KCoreDecomposition() = default;
    KCoreDecomposition(const KCoreDecomposition& other) : GDSAlgorithm{other} {}

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
        auto frontierPair = std::make_unique<KCoreFrontierPair>(
            graph->getNumNodesMap(clientContext->getTx()), totalNumNodes,
            clientContext->getMaxNumThreadForExec(), clientContext->getMemoryManager());
        auto initEdgeCompute = std::make_unique<KCoreInitEdgeCompute>(frontierPair.get());
        auto edgeCompute = std::make_unique<KCoreEdgeCompute>(frontierPair.get());
        auto computeState =
            RJCompState(std::move(frontierPair), std::move(edgeCompute), nullptr, nullptr);
        GDSUtils::initKCore(context, computeState, graph, std::move(initEdgeCompute));
        GDSUtils::runFrontiersUntilConvergence(context, computeState, graph, ExtendDirection::BOTH,
            2);
        auto vertexCompute = std::make_unique<KCoreVertexCompute>(clientContext->getMemoryManager(),
            sharedState.get(), computeState);
        GDSUtils::runVertexComputeIteration(context, sharedState->graph.get(), *vertexCompute);
        sharedState->mergeLocalTables();
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<KCoreDecomposition>(*this);
    }
};
function_set KCoreDecompositionFunction::getFunctionSet() {
    function_set result;
    auto algo = std::make_unique<KCoreDecomposition>();
    auto function =
        std::make_unique<GDSFunction>(name, algo->getParameterTypeIDs(), std::move(algo));
    result.push_back(std::move(function));
    return result;
}
} // namespace function
} // namespace kuzu