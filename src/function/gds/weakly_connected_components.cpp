#include "binder/binder.h"
#include "common/types/types.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds_function.h"
#include "graph/graph.h"
#include "processor/execution_context.h"
#include "processor/result/factorized_table.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/rec_joins.h"
#include "function/gds/gds_utils.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

class WCCFrontier : public GDSFrontier {
    using vertex_value_t = uint64_t;

public:
    WCCFrontier(const table_id_map_t<offset_t>& numNodesMap, storage::MemoryManager* mm) : GDSFrontier{numNodesMap} {
        KU_ASSERT(numNodesMap.size() == 1);
        numNodes = numNodesMap.begin()->second;
        vertexValueBuffer = mm->allocateBuffer(false, numNodes * sizeof(std::atomic<vertex_value_t>));
        vertexValues =reinterpret_cast<std::atomic<vertex_value_t>*>(vertexValueBuffer->getData());
        curActiveNodesBuffer = mm->allocateBuffer(false, numNodes * sizeof(std::atomic<bool>));
        curActiveNodes = reinterpret_cast<std::atomic<bool>*>(curActiveNodesBuffer->getData());
        nextActiveNodesBuffer = mm->allocateBuffer(false, numNodes * sizeof(std::atomic<bool>));
        nextActiveNodes = reinterpret_cast<std::atomic<bool>*>(nextActiveNodesBuffer->getData());
        for (auto i = 0u; i < numNodes; ++i) {
            vertexValues[i].store(i, std::memory_order_relaxed);
            curActiveNodes[i].store(true, std::memory_order_relaxed);
            nextActiveNodes[i].store(true, std::memory_order_relaxed);
        }
    }

    offset_t getNumNodes() const { return numNodes; }
    uint64_t getComponentID(offset_t offset) const { return vertexValues[offset]; }

    void pinTableID(table_id_t) override {}
    void setActive(std::span<const nodeID_t> nodeIDs) override {
        for (auto& nodeID : nodeIDs) {
            nextActiveNodes[nodeID.offset].store(true, std::memory_order_relaxed);
        }
    }
    void setActive(nodeID_t nodeID) override {
        nextActiveNodes[nodeID.offset].store(true, std::memory_order_relaxed);
    }

    bool isActive(offset_t offset) override {
        return curActiveNodes[offset].load(std::memory_order_relaxed);
    }

    // If bound node has smaller component ID set nbr node component ID to this smaller value.
    bool update(nodeID_t boundNodeID, nodeID_t nbrNodeID) {
        auto boundValue = vertexValues[boundNodeID.offset].load(std::memory_order_relaxed);
        auto tmp = vertexValues[nbrNodeID.offset].load(std::memory_order_relaxed);
        while (tmp > boundValue) {
            if (vertexValues[nbrNodeID.offset].compare_exchange_strong(tmp, boundValue)) {
                return true;
            }
        }
        return false;
    }

    void swapActiveNodes() {
        for (auto i = 0u; i < numNodes; ++i) {
            curActiveNodes[i].store(nextActiveNodes[i].load(std::memory_order_relaxed));
            nextActiveNodes[i].store(false, std::memory_order_relaxed);
        }
    }

    bool hasActiveNodes() const {
        for (auto i = 0u; i < numNodes; ++i) {
            if (nextActiveNodes[i].load(std::memory_order_relaxed)) {
                return true;
            }
        }
        return false;
    }

private:
    common::offset_t numNodes;
    std::unique_ptr<storage::MemoryBuffer> vertexValueBuffer;
    std::atomic<vertex_value_t>* vertexValues;
    std::unique_ptr<storage::MemoryBuffer> curActiveNodesBuffer;
    std::unique_ptr<storage::MemoryBuffer> nextActiveNodesBuffer;
    std::atomic<bool>* curActiveNodes;

public:

    std::atomic<bool>* nextActiveNodes;
};

class WCCFrontierPair : public FrontierPair {
public:
    WCCFrontierPair(std::shared_ptr<WCCFrontier> frontier, offset_t numNodes, uint64_t numThreads)
        : FrontierPair{frontier, frontier, numNodes, numThreads}, frontier{frontier}, morselDispatcher{numThreads} {}

    bool getNextRangeMorsel(FrontierMorsel& morsel) override {
        return morselDispatcher.getNextRangeMorsel(morsel);
    }

    void initRJFromSource(common::nodeID_t) override {}

    void beginFrontierComputeBetweenTables(table_id_t curTableID, table_id_t) override {
        morselDispatcher.init(curTableID, frontier->getNumNodes());
    }

    bool hasActiveNodesForNextLevel() override {
        return frontier->hasActiveNodes();
    }

    void beginNewIterationInternalNoLock() override {
        frontier->swapActiveNodes();
    }

    std::shared_ptr<WCCFrontier> frontier;

private:
    FrontierMorselDispatcher morselDispatcher;
};

struct WCCEdgeCompute : public EdgeCompute {
    WCCFrontierPair* frontierPair;

    explicit WCCEdgeCompute(WCCFrontierPair* frontierPair) : frontierPair{frontierPair} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::GraphScanState::Chunk &chunk, bool) override {
        std::vector<nodeID_t> result;
        chunk.forEach([&](auto nbrNodeID, auto) {
            if (frontierPair->frontier->update(boundNodeID, nbrNodeID)) {
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

    void materialize(nodeID_t nodeID, const WCCFrontier& frontier, FactorizedTable& table) const {
        nodeIDVector->setValue<nodeID_t>(0, nodeID);
        componentIDVector->setValue<uint64_t>(0, frontier.getComponentID(nodeID.offset));
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
    WCCVertexCompute(storage::MemoryManager* mm, processor::GDSCallSharedState* sharedState, const WCCFrontier& frontier)
        : mm{mm}, sharedState{sharedState}, frontier{frontier}, outputWriter{mm} {
        localFT = sharedState->claimLocalTable(mm);
    }
    ~WCCVertexCompute() override { sharedState->returnLocalTable(localFT); }

    bool beginOnTable(table_id_t) override {
        return true;
    }

    void vertexCompute(common::nodeID_t curNodeID) override {
        outputWriter.materialize(curNodeID, frontier, *localFT);
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<WCCVertexCompute>(mm, sharedState, frontier);
    }

private:
    storage::MemoryManager* mm;
    processor::GDSCallSharedState* sharedState;
    const WCCFrontier& frontier;
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
        KU_ASSERT(nodeTableIDs.size() == 1);
        auto nodeTableID = nodeTableIDs[0];
        auto numNodes = graph->getNumNodes(clientContext->getTx(), nodeTableID);
        auto frontier = std::make_shared<WCCFrontier>(graph->getNumNodesMap(clientContext->getTx()), clientContext->getMemoryManager());
        auto frontierPair = std::make_unique<WCCFrontierPair>(frontier, numNodes, clientContext->getMaxNumThreadForExec());
        auto edgeCompute = std::make_unique<WCCEdgeCompute>(frontierPair.get());
        auto computeState = RJCompState(std::move(frontierPair), std::move(edgeCompute), nullptr, nullptr);
        // TODO(Xiyang): fix upper Bound
        GDSUtils::runFrontiersUntilConvergence(context, computeState, graph, ExtendDirection::FWD, 100 /* upperBound */);
        auto vertexCompute = WCCVertexCompute(clientContext->getMemoryManager(), sharedState.get(), *frontier);
        GDSUtils::runVertexComputeIteration(context, sharedState->graph.get(),
            vertexCompute);
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
