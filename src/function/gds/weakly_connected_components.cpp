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

class WCCFrontier : public GDSFrontier {
public:
    WCCFrontier(const table_id_map_t<offset_t>& numNodesMap_, storage::MemoryManager* mm)
        : GDSFrontier{numNodesMap_} {
        curIter = 0;
        activeNodesInNextFrontier = 1;
        numNodes = 0;
        uint64_t componentIDCounter = 0;
        for (const auto& [tableID, curNumNodes] : numNodesMap_) {
            numNodesMap[tableID] = curNumNodes;
            numNodes += curNumNodes;
            auto memBuffer = mm->allocateBuffer(false, curNumNodes * sizeof(std::atomic<uint64_t>));
            auto curActiveMemBuffer =
                mm->allocateBuffer(false, curNumNodes * sizeof(std::atomic<bool>));
            auto nextActiveMemBuffer =
                mm->allocateBuffer(false, curNumNodes * sizeof(std::atomic<bool>));
            std::atomic<uint64_t>* memBufferPtr =
                reinterpret_cast<std::atomic<uint64_t>*>(memBuffer.get()->getData());
            std::atomic<bool>* curActiveMemBufferPtr =
                reinterpret_cast<std::atomic<bool>*>(curActiveMemBuffer.get()->getData());
            std::atomic<bool>* nextActiveMemBufferPtr =
                reinterpret_cast<std::atomic<bool>*>(nextActiveMemBuffer.get()->getData());
            // Cast a unique number to each node
            for (uint64_t i = 0; i < curNumNodes; ++i) {
                memBufferPtr[i].store(componentIDCounter, std::memory_order_relaxed);
                curActiveMemBufferPtr[i].store(true, std::memory_order_relaxed);
                nextActiveMemBufferPtr[i].store(false, std::memory_order_relaxed);
                componentIDCounter++;
            }
            vertexValues.insert({tableID, std::move(memBuffer)});
            curActiveNodes.insert({tableID, std::move(curActiveMemBuffer)});
            nextActiveNodes.insert({tableID, std::move(nextActiveMemBuffer)});
        }
    }

    uint64_t getCurIter() const { return curIter; }

    uint64_t getNumNodes() const { return numNodes; }

    uint64_t getComponentID(common::nodeID_t nodeID) const {
        return getComponentIDAtomic(nodeID).load(std::memory_order_relaxed);
    }

    void setActive(std::span<const common::nodeID_t> nodeIDs) override {
        for (auto& nodeID : nodeIDs) {
            setNextActive(nodeID);
        }
    }

    void setActive(common::nodeID_t nodeID) { setNextActive(nodeID); }

    // pinTableID and isActive is not used, most likely needs refactoring
    void pinTableID(table_id_t) override {}
    bool isActive(offset_t) override { return true; }

    bool hasNextActive() { return activeNodesInNextFrontier > 0; }

    bool update(nodeID_t boundNodeID, nodeID_t nbrNodeID) {
        auto& boundComponentID = getComponentIDAtomic(boundNodeID);
        auto& nbrComponentID = getComponentIDAtomic(nbrNodeID);

        auto expectedComponentID = boundComponentID.load(std::memory_order_relaxed);
        auto actualComponentID = nbrComponentID.load(std::memory_order_relaxed);

        // If needs changing
        if (expectedComponentID < actualComponentID) {
            activeNodesInNextFrontier++;
            setNextActive(nbrNodeID);
            while (!nbrComponentID.compare_exchange_weak(actualComponentID, expectedComponentID,
                std::memory_order_relaxed, std::memory_order_relaxed)) {
                actualComponentID = nbrComponentID.load(std::memory_order_relaxed);
            }
            return true;
        }
        return false;
    }

    void swapActiveNodes() {
        activeNodesInNextFrontier = 0;
        curIter++;
        for (const auto& [tableID, curNumNodes] : numNodesMap) {
            auto& curMemBuffer = curActiveNodes.find(tableID)->second;
            auto& nextMemBuffer = nextActiveNodes.find(tableID)->second;
            std::atomic<bool>* curActiveMemBufferPtr =
                reinterpret_cast<std::atomic<bool>*>(curMemBuffer.get()->getData());
            std::atomic<bool>* nextActiveMemBufferPtr =
                reinterpret_cast<std::atomic<bool>*>(nextMemBuffer.get()->getData());
            for (auto i = 0u; i < curNumNodes; ++i) {
                curActiveMemBufferPtr[i].store(
                    nextActiveMemBufferPtr[i].load(std::memory_order_relaxed),
                    std::memory_order_relaxed);
                nextActiveMemBufferPtr[i].store(false, std::memory_order_relaxed);
            }
        }
    }

private:
    void setNextActive(common::nodeID_t nodeID) {
        activeNodesInNextFrontier++;
        auto& memBuffer = nextActiveNodes.find(nodeID.tableID)->second;
        std::atomic<bool>* memBufferPtr =
            reinterpret_cast<std::atomic<bool>*>(memBuffer->getData());
        memBufferPtr[nodeID.offset].store(static_cast<bool>(true), std::memory_order_relaxed);
    }
    std::atomic<uint64_t>& getComponentIDAtomic(common::nodeID_t nodeID) const {
        auto& memBuffer = vertexValues.find(nodeID.tableID)->second;
        std::atomic<uint64_t>* memBufferPtr =
            reinterpret_cast<std::atomic<uint64_t>*>(memBuffer->getData());
        return memBufferPtr[nodeID.offset];
    }

private:
    uint16_t curIter;
    uint64_t activeNodesInNextFrontier;
    uint64_t numNodes;
    common::table_id_map_t<std::unique_ptr<storage::MemoryBuffer>> vertexValues;
    common::table_id_map_t<std::unique_ptr<storage::MemoryBuffer>> curActiveNodes;
    common::table_id_map_t<std::unique_ptr<storage::MemoryBuffer>> nextActiveNodes;
};

class WCCFrontierPair : public FrontierPair {
public:
    // FrontierPair might also need refactoring - a lot of Algorithms are not using FrontierPair as
    // intended
    WCCFrontierPair(std::shared_ptr<WCCFrontier> frontier, uint64_t numNodes, uint64_t numThreads)
        : FrontierPair{frontier, frontier, numNodes, numThreads}, frontier{frontier},
          morselDispatcher{numThreads} {}

    bool getNextRangeMorsel(FrontierMorsel& morsel) override {
        return morselDispatcher.getNextRangeMorsel(morsel);
    }

    void initRJFromSource(common::nodeID_t) override {}

    void beginFrontierComputeBetweenTables(table_id_t curTableID, table_id_t) override {
        morselDispatcher.init(curTableID, frontier->getNumNodes());
    }

    bool hasActiveNodesForNextLevel() override { return frontier->hasNextActive(); }

    void beginNewIterationInternalNoLock() override { frontier->swapActiveNodes(); }

public:
    std::shared_ptr<WCCFrontier> frontier;

private:
    FrontierMorselDispatcher morselDispatcher;
};

struct WCCEdgeCompute : public EdgeCompute {
    WCCFrontierPair* frontierPair;

    explicit WCCEdgeCompute(WCCFrontierPair* frontierPair) : frontierPair{frontierPair} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk, bool) override {
        std::vector<nodeID_t> result;
        chunk.forEach([&](auto nbrNodeID, auto) {
            if (frontierPair->frontier->update(boundNodeID, nbrNodeID)) {
                result.push_back(nbrNodeID);
            }
        });
        return result;
    }

    void resetSingleThreadState() override { return; }

    bool terminate(processor::NodeOffsetMaskMap& maskMap) override {
        return false;
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
        componentIDVector->setValue<uint64_t>(0, frontier.getComponentID(nodeID));
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
        const WCCFrontier& frontier)
        : mm{mm}, sharedState{sharedState}, frontier{frontier}, outputWriter{mm} {
        localFT = sharedState->claimLocalTable(mm);
    }
    ~WCCVertexCompute() override { sharedState->returnLocalTable(localFT); }

    bool beginOnTable(common::table_id_t) override { return true; }

    void vertexCompute(const graph::VertexScanState::Chunk& chunk) override {
        for (auto nodeID : chunk.getNodeIDs()) {
            outputWriter.materialize(nodeID, frontier, *localFT);
        }
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
        uint64_t totalNumNodes = 0;
        for (auto& nodeTableID : nodeTableIDs) {
            totalNumNodes += graph->getNumNodes(clientContext->getTx(), nodeTableID);
        }
        auto frontier = std::make_shared<WCCFrontier>(graph->getNumNodesMap(clientContext->getTx()),
            clientContext->getMemoryManager());
        auto frontierPair = std::make_unique<WCCFrontierPair>(frontier, totalNumNodes,
            clientContext->getMaxNumThreadForExec());
        auto edgeCompute = std::make_unique<WCCEdgeCompute>(frontierPair.get());
        // GDS::Utils::RunUntilConvergence shouldn't explicitly call RJCompState since other
        // algorithms can also use RunUntilConvergence. A solution could be to have a general
        // CompState class which RJCompState derives from.
        auto computeState =
            RJCompState(std::move(frontierPair), std::move(edgeCompute), nullptr, nullptr);
        GDSUtils::runFrontiersUntilConvergence(context, computeState, graph, ExtendDirection::FWD,
            10);
        auto vertexCompute = std::make_unique<WCCVertexCompute>(clientContext->getMemoryManager(),
            sharedState.get(), *frontier);
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
