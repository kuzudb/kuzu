#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "degrees.h"
#include "function/gds/gds.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/gds_utils.h"
#include "function/gds/output_writer.h"
#include "function/gds_function.h"
#include "graph/graph.h"
#include "main/client_context.h"
#include "processor/execution_context.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

struct PageRankBindData final : public GDSBindData {
    double dampingFactor = 0.85;
    int64_t maxIteration = 20;
    double delta = 0.0000001; // Convergence delta

    explicit PageRankBindData(graph::GraphEntry graphEntry,
        std::shared_ptr<binder::Expression> nodeOutput)
        : GDSBindData{std::move(graphEntry), std::move(nodeOutput)} {};
    PageRankBindData(const PageRankBindData& other)
        : GDSBindData{other}, dampingFactor{other.dampingFactor}, maxIteration{other.maxIteration},
          delta{other.delta} {}

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<PageRankBindData>(*this);
    }
};

static void addCAS(std::atomic<double>& origin, double valToAdd) {
    auto expected = origin.load(std::memory_order_relaxed);
    auto desired = expected + valToAdd;
    while (!origin.compare_exchange_strong(expected, desired)) {
        desired = expected + valToAdd;
    }
}

// Represents PageRank value for all nodes
class PValues {
public:
    PValues(table_id_map_t<offset_t> numNodesMap, storage::MemoryManager* mm, double val) {
        for (const auto& [tableID, numNodes] : numNodesMap) {
            valueMap.allocate(tableID, numNodes, mm);
            pin(tableID);
            for (auto i = 0u; i < numNodes; ++i) {
                values[i].store(val, std::memory_order_relaxed);
            }
        }
    }

    void pin(table_id_t tableID) { values = valueMap.getData(tableID); }

    double getValue(offset_t offset) { return values[offset].load(std::memory_order_relaxed); }

    void addValueCAS(offset_t offset, double val) { addCAS(values[offset], val); }

    void setValue(offset_t offset, double val) {
        values[offset].store(val, std::memory_order_relaxed);
    }

private:
    std::atomic<double>* values = nullptr;
    ObjectArraysMap<std::atomic<double>> valueMap;
};

// Sum the weight (current rank / degree) for each incoming edge.
class PNextUpdateEdgeCompute : public EdgeCompute {
public:
    PNextUpdateEdgeCompute(Degrees* degrees, PValues* pCurrent, PValues* pNext)
        : degrees{degrees}, pCurrent{pCurrent}, pNext{pNext} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk,
        bool) override {
        if (chunk.size() > 0) {
            double valToAdd = 0;
            chunk.forEach([&](auto nbrNodeID, auto) {
                valToAdd +=
                    pCurrent->getValue(nbrNodeID.offset) / degrees->getValue(nbrNodeID.offset);
            });
            pNext->addValueCAS(boundNodeID.offset, valToAdd);
        }
        return {boundNodeID};
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<PNextUpdateEdgeCompute>(degrees, pCurrent, pNext);
    }

private:
    Degrees* degrees;
    PValues* pCurrent;
    PValues* pNext;
};

// Evaluate rank = above result * dampingFactor + {(1 - dampingFactor) / |V|} (constant)
class PNextUpdateVertexCompute : public VertexCompute {
public:
    PNextUpdateVertexCompute(double dampingFactor, double constant, PValues* pNext)
        : dampingFactor{dampingFactor}, constant{constant}, pNext{pNext} {}

    bool beginOnTable(table_id_t tableID) override {
        pNext->pin(tableID);
        return true;
    }

    void vertexCompute(offset_t startOffset, offset_t endOffset, table_id_t) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            pNext->setValue(i, pNext->getValue(i) * dampingFactor + constant);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<PNextUpdateVertexCompute>(dampingFactor, constant, pNext);
    }

private:
    double dampingFactor;
    double constant;
    PValues* pNext;
};

class PDiffVertexCompute : public VertexCompute {
public:
    PDiffVertexCompute(std::atomic<double>& diff, PValues* pCurrent, PValues* pNext)
        : diff{diff}, pCurrent{pCurrent}, pNext{pNext} {}

    bool beginOnTable(table_id_t tableID) override {
        pCurrent->pin(tableID);
        pNext->pin(tableID);
        return true;
    }

    void vertexCompute(offset_t startOffset, offset_t endOffset, table_id_t) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            auto next = pNext->getValue(i);
            auto current = pCurrent->getValue(i);
            if (next > current) {
                addCAS(diff, next - current);
            } else {
                addCAS(diff, current - next);
            }
            pCurrent->setValue(i, 0);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<PDiffVertexCompute>(diff, pCurrent, pNext);
    }

private:
    std::atomic<double>& diff;
    PValues* pCurrent;
    PValues* pNext;
};

class PageRankOutputWriter : public GDSOutputWriter {
public:
    PageRankOutputWriter(main::ClientContext* context, processor::NodeOffsetMaskMap* outputNodeMask,
        PValues* pNext)
        : GDSOutputWriter{context, outputNodeMask}, pNext{pNext} {
        nodeIDVector = createVector(LogicalType::INTERNAL_ID(), context->getMemoryManager());
        rankVector = createVector(LogicalType::DOUBLE(), context->getMemoryManager());
    }

    void pinTableID(table_id_t tableID) override {
        GDSOutputWriter::pinTableID(tableID);
        pNext->pin(tableID);
    }

    void materialize(offset_t startOffset, offset_t endOffset, table_id_t tableID,
        FactorizedTable& table) const {
        for (auto i = startOffset; i < endOffset; ++i) {
            auto nodeID = nodeID_t{i, tableID};
            nodeIDVector->setValue<nodeID_t>(0, nodeID);
            rankVector->setValue<double>(0, pNext->getValue(i));
            table.append(vectors);
        }
    }

    std::unique_ptr<PageRankOutputWriter> copy() const {
        return std::make_unique<PageRankOutputWriter>(context, outputNodeMask, pNext);
    }

private:
    std::unique_ptr<ValueVector> nodeIDVector;
    std::unique_ptr<ValueVector> rankVector;
    PValues* pNext;
};

class OutputVertexCompute : public VertexCompute {
public:
    OutputVertexCompute(storage::MemoryManager* mm, processor::GDSCallSharedState* sharedState,
        std::unique_ptr<PageRankOutputWriter> outputWriter)
        : mm{mm}, sharedState{sharedState}, outputWriter{std::move(outputWriter)} {
        localFT = sharedState->claimLocalTable(mm);
    }
    ~OutputVertexCompute() override { sharedState->returnLocalTable(localFT); }

    bool beginOnTable(table_id_t tableID) override {
        outputWriter->pinTableID(tableID);
        return true;
    }

    void vertexCompute(offset_t startOffset, offset_t endOffset, table_id_t tableID) override {
        outputWriter->materialize(startOffset, endOffset, tableID, *localFT);
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<OutputVertexCompute>(mm, sharedState, outputWriter->copy());
    }

private:
    storage::MemoryManager* mm;
    processor::GDSCallSharedState* sharedState;
    std::unique_ptr<PageRankOutputWriter> outputWriter;
    processor::FactorizedTable* localFT;
};

class PageRank final : public GDSAlgorithm {
    static constexpr char RANK_COLUMN_NAME[] = "rank";

public:
    PageRank() = default;
    PageRank(const PageRank& other) : GDSAlgorithm{other} {}

    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return {LogicalTypeID::ANY};
    }

    binder::expression_vector getResultColumns(binder::Binder* binder) const override {
        expression_vector columns;
        auto& outputNode = bindData->getNodeOutput()->constCast<NodeExpression>();
        columns.push_back(outputNode.getInternalID());
        columns.push_back(binder->createVariable(RANK_COLUMN_NAME, LogicalType::DOUBLE()));
        return columns;
    }

    void bind(const GDSBindInput& input, main::ClientContext& context) override {
        auto graphName = binder::ExpressionUtil::getLiteralValue<std::string>(*input.getParam(0));
        auto graphEntry = bindGraphEntry(context, graphName);
        auto nodeOutput = bindNodeOutput(input.binder, graphEntry.nodeEntries);
        bindData = std::make_unique<PageRankBindData>(std::move(graphEntry), nodeOutput);
    }

    void exec(processor::ExecutionContext* context) override {
        auto clientContext = context->clientContext;
        auto transaction = clientContext->getTransaction();
        auto graph = sharedState->graph.get();
        auto numNodesMap = graph->getNumNodesMap(transaction);
        auto numNodes = graph->getNumNodes(transaction);
        auto p1 = PValues(numNodesMap, clientContext->getMemoryManager(), (double)1 / numNodes);
        auto p2 = PValues(numNodesMap, clientContext->getMemoryManager(), 0);
        PValues* pCurrent = &p1;
        PValues* pNext = &p2;
        auto pageRankBindData = bindData->ptrCast<PageRankBindData>();
        auto currentIter = 1u;
        auto currentFrontier = getPathLengthsFrontier(context, PathLengths::UNVISITED);
        auto nextFrontier = getPathLengthsFrontier(context, 0);
        auto degrees = Degrees(numNodesMap, clientContext->getMemoryManager());
        DegreesUtils::computeDegree(context, graph, &degrees, ExtendDirection::FWD);
        auto frontierPair =
            std::make_unique<DoublePathLengthsFrontierPair>(currentFrontier, nextFrontier);
        frontierPair->setActiveNodesForNextIter();
        frontierPair->getNextSparseFrontier().disable();
        auto computeState =
            GDSComputeState(std::move(frontierPair), nullptr, sharedState->getOutputNodeMaskMap());
        auto pNextUpdateConstant = (1 - pageRankBindData->dampingFactor) * ((double)1 / numNodes);
        while (currentIter < pageRankBindData->maxIteration) {
            auto ec = std::make_unique<PNextUpdateEdgeCompute>(&degrees, pCurrent, pNext);
            computeState.edgeCompute = std::move(ec);
            GDSUtils::runFrontiersUntilConvergence(context, computeState, graph,
                ExtendDirection::BWD, computeState.frontierPair->getCurrentIter() + 1);
            auto pNextUpdateVC = PNextUpdateVertexCompute(pageRankBindData->dampingFactor,
                pNextUpdateConstant, pNext);
            GDSUtils::runVertexCompute(context, graph, pNextUpdateVC);
            std::atomic<double> diff;
            diff.store(0);
            auto pDiffVC = PDiffVertexCompute(diff, pCurrent, pNext);
            GDSUtils::runVertexCompute(context, graph, pDiffVC);
            std::swap(pCurrent, pNext);
            if (diff.load() < pageRankBindData->delta) { // Converged.
                break;
            }
            currentIter++;
        }
        auto writer = std::make_unique<PageRankOutputWriter>(clientContext,
            sharedState->getOutputNodeMaskMap(), pCurrent);
        auto outputVC = std::make_unique<OutputVertexCompute>(clientContext->getMemoryManager(),
            sharedState.get(), std::move(writer));
        GDSUtils::runVertexCompute(context, graph, *outputVC);
        sharedState->mergeLocalTables();
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<PageRank>(*this);
    }

private:
    std::unique_ptr<PageRankOutputWriter> localState;
};

function_set PageRankFunction::getFunctionSet() {
    function_set result;
    auto algo = std::make_unique<PageRank>();
    auto function =
        std::make_unique<GDSFunction>(name, algo->getParameterTypeIDs(), std::move(algo));
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
