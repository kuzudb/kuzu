#include "binder/binder.h"
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

class KUZU_API KCoreFrontierPair : public FrontierPair {
public:
    KCoreFrontierPair(std::shared_ptr<GDSFrontier> curFrontier,
        std::shared_ptr<GDSFrontier> nextFrontier, uint64_t maxThreads,
        table_id_map_t<offset_t> numNodesMap, storage::MemoryManager* mm)
        : FrontierPair(curFrontier, nextFrontier, maxThreads), numNodesMap{numNodesMap} {
        for (const auto& [tableID, curNumNodes] : numNodesMap) {
            curVertexValues.allocate(tableID, curNumNodes, mm);
            auto data = curVertexValues.getData(tableID);
            for (auto i = 0u; i < curNumNodes; ++i) {
                data[i].store(0, std::memory_order_relaxed);
            }
        }
    }

    void initRJFromSource(common::nodeID_t /* source */) override { setActiveNodesForNextIter(); };

    void pinCurrFrontier(common::table_id_t tableID) override {
        FrontierPair::pinCurrFrontier(tableID);
        curDenseFrontier->ptrCast<PathLengths>()->pinCurFrontierTableID(tableID);
    }

    void pinNextFrontier(common::table_id_t tableID) override {
        FrontierPair::pinNextFrontier(tableID);
        nextDenseFrontier->ptrCast<PathLengths>()->pinNextFrontierTableID(tableID);
    }

    void pinVertexValues(common::table_id_t tableID) {
        vertexValues = curVertexValues.getData(tableID);
    }

    void beginFrontierComputeBetweenTables(table_id_t curTableID, table_id_t nextTableID) override {
        FrontierPair::beginFrontierComputeBetweenTables(curTableID, nextTableID);
        pinVertexValues(nextTableID);
    }

    void beginNewIterationInternalNoLock() override {
        std::swap(curDenseFrontier, nextDenseFrontier);
        updateSmallestDegree();
        curDenseFrontier->ptrCast<PathLengths>()->incrementCurIter();
        nextDenseFrontier->ptrCast<PathLengths>()->incrementCurIter();
    }

    uint64_t addToVertexDegree(common::nodeID_t nodeID, uint64_t degreeToAdd) {
        return curVertexValues.getData(nodeID.tableID)[nodeID.offset].fetch_add(degreeToAdd,
            std::memory_order_relaxed);
    }

    uint64_t removeFromVertex(common::nodeID_t nodeID) {
        int curSmallest = curSmallestDegree.load(std::memory_order_relaxed);
        int curVertexDegree =
            curVertexValues.getData(nodeID.tableID)[nodeID.offset].load(std::memory_order_relaxed);
        if (curVertexDegree > curSmallest) {
            return curVertexValues.getData(nodeID.tableID)[nodeID.offset].fetch_sub(1,
                std::memory_order_relaxed);
        }
        return curVertexDegree;
    }

    void updateSmallestDegree(bool dontCheckOffset = false) {
        if (updateDegreeFlag) {
            updateDegreeFlag = false;
            return;
        }
        uint64_t curSmallest = UINT64_MAX;
        for (const auto& [tableID, curNumNodes] : numNodesMap) {
            curDenseFrontier->pinTableID(tableID);
            for (uint64_t offset = 0; offset < curNumNodes; ++offset) {
                if (dontCheckOffset || curDenseFrontier->isActive(offset)) {
                    curSmallest = std::min(curSmallest,
                        curVertexValues.getData(tableID)[offset].load(std::memory_order_relaxed));
                }
            }
        }
        uint64_t current = curSmallestDegree.load(std::memory_order_relaxed);
        curSmallestDegree.compare_exchange_strong(current, curSmallest, std::memory_order_relaxed);
    }

    void setAllActive() {
        for (const auto& [tableID, curNumNodes] : numNodesMap) {
            curDenseFrontier->pinTableID(tableID);
            for (uint64_t offset = 0; offset < curNumNodes; ++offset) {
                common::nodeID_t nodeID;
                nodeID.tableID = tableID;
                nodeID.offset = offset;
                curDenseFrontier->setActive(nodeID);
            }
        }
    }

    uint64_t getSmallestDegree() { return curSmallestDegree.load(std::memory_order_relaxed); }

    void setUpdateFlag() { updateDegreeFlag = true; }

    bool isNodeActive(common::nodeID_t nodeID) const {
        return curDenseFrontier->isActive(nodeID.offset);
    }

    uint64_t getVertexValue(common::offset_t offset) {
        return vertexValues[offset].load(std::memory_order_relaxed);
    }

private:
    bool updateDegreeFlag = false;
    std::atomic<uint64_t> curSmallestDegree{UINT64_MAX};
    common::table_id_map_t<common::offset_t> numNodesMap;
    std::atomic<uint64_t>* vertexValues = nullptr;
    ObjectArraysMap<std::atomic<uint64_t>> curVertexValues;
};

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
        uint64_t vertexDegree = frontierPair->getVertexValue(boundNodeID.offset);
        uint64_t smallestDegree = frontierPair->getSmallestDegree();
        if (frontierPair->isNodeActive(boundNodeID) && (vertexDegree == smallestDegree)) {
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

class KCoreOutputWriter : GDSOutputWriter {
public:
    explicit KCoreOutputWriter(main::ClientContext* context,
        processor::NodeOffsetMaskMap* outputNodeMask, KCoreFrontierPair* frontierPair)
        : GDSOutputWriter{context, outputNodeMask}, frontierPair{frontierPair} {
        nodeIDVector = createVector(LogicalType::INTERNAL_ID(), context->getMemoryManager());
        kValueVector = createVector(LogicalType::UINT64(), context->getMemoryManager());
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
            kValueVector->setValue<uint64_t>(0, frontierPair->getVertexValue(i));
            table.append(vectors);
        }
    }

    std::unique_ptr<KCoreOutputWriter> copy() const {
        return std::make_unique<KCoreOutputWriter>(context, outputNodeMask, frontierPair);
    }

private:
    std::unique_ptr<ValueVector> nodeIDVector;
    std::unique_ptr<ValueVector> kValueVector;
    KCoreFrontierPair* frontierPair;
};

class KCoreVertexCompute : public VertexCompute {
public:
    KCoreVertexCompute(storage::MemoryManager* mm, processor::GDSCallSharedState* sharedState,
        std::unique_ptr<KCoreOutputWriter> outputWriter)
        : mm{mm}, sharedState{sharedState}, outputWriter{std::move(outputWriter)} {
        localFT = sharedState->claimLocalTable(mm);
    }
    ~KCoreVertexCompute() override { sharedState->returnLocalTable(localFT); }

    bool beginOnTable(common::table_id_t tableID) override {
        outputWriter->pinTableID(tableID);
        return true;
    }

    void vertexCompute(common::offset_t startOffset, common::offset_t endOffset,
        common::table_id_t tableID) override {
        outputWriter->materialize(startOffset, endOffset, tableID, *localFT);
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<KCoreVertexCompute>(mm, sharedState, outputWriter->copy());
    }

private:
    storage::MemoryManager* mm;
    processor::GDSCallSharedState* sharedState;
    std::unique_ptr<KCoreOutputWriter> outputWriter;
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
        auto numNodesMap = graph->getNumNodesMap(clientContext->getTx());
        auto numThreads = clientContext->getMaxNumThreadForExec();
        auto currentFrontier = getPathLengthsFrontier(context, PathLengths::UNVISITED);
        auto nextFrontier = getPathLengthsFrontier(context, 0);
        auto frontierPair = std::make_unique<KCoreFrontierPair>(currentFrontier, nextFrontier,
            numThreads, numNodesMap, clientContext->getMemoryManager());

        frontierPair->setActiveNodesForNextIter();
        frontierPair->getNextSparseFrontier().disable();
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