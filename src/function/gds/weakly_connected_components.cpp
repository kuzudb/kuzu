#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "common/types/types.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/gds_object_manager.h"
#include "function/gds/gds_utils.h"
#include "function/gds/output_writer.h"
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

class WCCFrontierPair : public FrontierPair {
public:
    WCCFrontierPair(std::shared_ptr<GDSFrontier> curFrontier,
        std::shared_ptr<GDSFrontier> nextFrontier, uint64_t maxThreads,
        table_id_map_t<offset_t> numNodesMap, storage::MemoryManager* mm)
        : FrontierPair(curFrontier, nextFrontier, maxThreads) {
        for (const auto& [tableID, numNodes] : numNodesMap) {
            vertexValueMap.allocate(tableID, numNodes, mm);
            auto data = vertexValueMap.getData(tableID);
            // Cast a unique number to each node
            for (auto i = 0u; i < numNodes; ++i) {
                data[i].store(i, std::memory_order_relaxed);
            }
        }
    }

    void initRJFromSource(common::nodeID_t) override { setActiveNodesForNextIter(); };

    void pinCurrFrontier(common::table_id_t tableID) override {
        FrontierPair::pinCurrFrontier(tableID);
        curDenseFrontier->ptrCast<PathLengths>()->pinCurFrontierTableID(tableID);
    }
    void pinNextFrontier(common::table_id_t tableID) override {
        FrontierPair::pinNextFrontier(tableID);
        nextDenseFrontier->ptrCast<PathLengths>()->pinNextFrontierTableID(tableID);
    }
    void pinVertexValues(common::table_id_t tableID) {
        vertexValues = vertexValueMap.getData(tableID);
    }

    void beginFrontierComputeBetweenTables(common::table_id_t currentTableID,
        common::table_id_t nextTableID) override {
        FrontierPair::beginFrontierComputeBetweenTables(currentTableID, nextTableID);
        pinVertexValues(nextTableID);
    }

    void beginNewIterationInternalNoLock() override {
        std::swap(curDenseFrontier, nextDenseFrontier);
        curDenseFrontier->ptrCast<PathLengths>()->incrementCurIter();
        nextDenseFrontier->ptrCast<PathLengths>()->incrementCurIter();
    }

    bool update(common::nodeID_t boundNodeID, common::nodeID_t nbrNodeID) {
        auto boundValue = vertexValues[boundNodeID.offset].load(std::memory_order_relaxed);
        auto tmp = vertexValues[nbrNodeID.offset].load(std::memory_order_relaxed);
        while (tmp > boundValue) {
            if (vertexValues[nbrNodeID.offset].compare_exchange_strong(tmp, boundValue)) {
                return true;
            }
        }
        return false;
    }

    common::offset_t getVertexValue(common::offset_t offset) {
        return vertexValues[offset].load(std::memory_order_relaxed);
    }

private:
    std::atomic<common::offset_t>* vertexValues = nullptr;
    ObjectArraysMap<std::atomic<common::offset_t>> vertexValueMap;
};

struct WCCEdgeCompute : public EdgeCompute {
    WCCFrontierPair& frontierPair;

    explicit WCCEdgeCompute(WCCFrontierPair& frontierPair) : frontierPair{frontierPair} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk,
        bool) override {
        std::vector<nodeID_t> result;
        chunk.forEach([&](auto nbrNodeID, auto) {
            if (frontierPair.update(boundNodeID, nbrNodeID)) {
                result.push_back(nbrNodeID);
            }
        });
        return result;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<WCCEdgeCompute>(frontierPair);
    }
};

class WCCOutputWriter : public GDSOutputWriter {
public:
    WCCOutputWriter(main::ClientContext* context, processor::NodeOffsetMaskMap* outputNodeMask,
        WCCFrontierPair* frontierPair)
        : GDSOutputWriter{context, outputNodeMask}, frontierPair{frontierPair} {
        nodeIDVector = createVector(LogicalType::INTERNAL_ID(), context->getMemoryManager());
        componentIDVector = createVector(LogicalType::UINT64(), context->getMemoryManager());
    }

    void pinTableID(common::table_id_t tableID) override {
        GDSOutputWriter::pinTableID(tableID);
        frontierPair->pinVertexValues(tableID);
    }

    void materialize(offset_t startOffset, offset_t endOffset, table_id_t tableID,
        FactorizedTable& table) const {
        for (auto i = startOffset; i < endOffset; ++i) {
            auto nodeID = nodeID_t{i, tableID};
            nodeIDVector->setValue<nodeID_t>(0, nodeID);
            componentIDVector->setValue<uint64_t>(0, frontierPair->getVertexValue(i));
            table.append(vectors);
        }
    }

    std::unique_ptr<WCCOutputWriter> copy() const {
        return std::make_unique<WCCOutputWriter>(context, outputNodeMask, frontierPair);
    }

private:
    std::unique_ptr<ValueVector> nodeIDVector;
    std::unique_ptr<ValueVector> componentIDVector;
    WCCFrontierPair* frontierPair;
};

class WCCVertexCompute : public VertexCompute {
public:
    WCCVertexCompute(storage::MemoryManager* mm, processor::GDSCallSharedState* sharedState,
        std::unique_ptr<WCCOutputWriter> outputWriter)
        : mm{mm}, sharedState{sharedState}, outputWriter{std::move(outputWriter)} {
        localFT = sharedState->claimLocalTable(mm);
    }
    ~WCCVertexCompute() override { sharedState->returnLocalTable(localFT); }

    bool beginOnTable(common::table_id_t tableID) override {
        outputWriter->pinTableID(tableID);
        return true;
    }

    void vertexCompute(common::offset_t startOffset, common::offset_t endOffset,
        common::table_id_t tableID) override {
        outputWriter->materialize(startOffset, endOffset, tableID, *localFT);
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<WCCVertexCompute>(mm, sharedState, outputWriter->copy());
    }

private:
    storage::MemoryManager* mm;
    processor::GDSCallSharedState* sharedState;
    std::unique_ptr<WCCOutputWriter> outputWriter;
    processor::FactorizedTable* localFT;
};

class WeaklyConnectedComponent final : public GDSAlgorithm {
    static constexpr char GROUP_ID_COLUMN_NAME[] = "group_id";
    static constexpr uint8_t MAX_ITERATION = 100;

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

    void bind(const GDSBindInput& input, main::ClientContext& context) override {
        auto graphName = binder::ExpressionUtil::getLiteralValue<std::string>(*input.getParam(0));
        auto graphEntry = bindGraphEntry(context, graphName);
        auto nodeOutput = bindNodeOutput(input.binder, graphEntry.nodeEntries);
        bindData = std::make_unique<GDSBindData>(std::move(graphEntry), nodeOutput);
    }

    void exec(processor::ExecutionContext* context) override {
        auto clientContext = context->clientContext;
        auto graph = sharedState->graph.get();
        auto numNodesMap = graph->getNumNodesMap(clientContext->getTx());
        auto numThreads = clientContext->getMaxNumThreadForExec();
        auto currentFrontier = getPathLengthsFrontier(context, PathLengths::UNVISITED);
        auto nextFrontier = getPathLengthsFrontier(context, 0);
        auto frontierPair = std::make_unique<WCCFrontierPair>(currentFrontier, nextFrontier,
            numThreads, numNodesMap, clientContext->getMemoryManager());
        // Initialize starting nodes in the next frontier.
        // When beginNewIteration, next frontier will become current frontier
        frontierPair->setActiveNodesForNextIter();
        frontierPair->getNextSparseFrontier().disable();
        auto edgeCompute = std::make_unique<WCCEdgeCompute>(*frontierPair.get());
        auto writer = std::make_unique<WCCOutputWriter>(clientContext,
            sharedState->getOutputNodeMaskMap(), frontierPair.get());
        auto computeState = GDSComputeState(std::move(frontierPair), std::move(edgeCompute),
            sharedState->getOutputNodeMaskMap());
        GDSUtils::runFrontiersUntilConvergence(context, computeState, graph, ExtendDirection::BOTH,
            MAX_ITERATION);
        auto vertexCompute = std::make_unique<WCCVertexCompute>(clientContext->getMemoryManager(),
            sharedState.get(), std::move(writer));
        GDSUtils::runVertexCompute(context, sharedState->graph.get(), *vertexCompute);
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
