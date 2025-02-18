#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "common/types/types.h"
#include "degrees.h"
#include "function/gds/auxiliary_state/gds_auxilary_state.h"
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

class CoreValues {
public:
    CoreValues(const table_id_map_t<offset_t>& numNodesMap, MemoryManager* mm) {
        init(numNodesMap, mm);
    }

    void pinTable(table_id_t tableID) { coreValues = coreValuesMap.getData(tableID); }

    bool isValid(offset_t offset) {
        return coreValues[offset].load(std::memory_order_relaxed) != INVALID_DEGREE;
    }
    degree_t getValue(offset_t offset) {
        return coreValues[offset].load(std::memory_order_relaxed);
    }
    void setCoreValue(offset_t offset, degree_t value) {
        coreValues[offset].store(value, std::memory_order_relaxed);
    }

private:
    void init(const table_id_map_t<offset_t>& numNodesMap, MemoryManager* mm) {
        for (const auto& [tableID, numNodes] : numNodesMap) {
            coreValuesMap.allocate(tableID, numNodes, mm);
            pinTable(tableID);
            for (auto i = 0u; i < numNodes; ++i) {
                coreValues[i].store(INVALID_DEGREE, std::memory_order_relaxed);
            }
        }
    }

private:
    std::atomic<degree_t>* coreValues = nullptr;
    ObjectArraysMap<std::atomic<degree_t>> coreValuesMap;
};

class KCoreAuxiliaryState : public GDSAuxiliaryState {
public:
    KCoreAuxiliaryState(Degrees& degrees, CoreValues& coreValues)
        : degrees{degrees}, coreValues{coreValues} {}

    void beginFrontierCompute(table_id_t, table_id_t toTableID) override {
        degrees.pinTable(toTableID);
        coreValues.pinTable(toTableID);
    }

private:
    Degrees& degrees;
    CoreValues& coreValues;
};

class RemoveVertexEdgeCompute : public EdgeCompute {
public:
    explicit RemoveVertexEdgeCompute(Degrees& degrees) : degrees{degrees} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t, graph::NbrScanState::Chunk& chunk, bool) override {
        chunk.forEach([&](auto nbrNodeID, auto) { degrees.decreaseDegreeByOne(nbrNodeID.offset); });
        return {};
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<RemoveVertexEdgeCompute>(degrees);
    }

private:
    Degrees& degrees;
};

class KCoreOutputWriter : public GDSOutputWriter {
public:
    explicit KCoreOutputWriter(main::ClientContext* context) : GDSOutputWriter{context} {
        nodeIDVector = createVector(LogicalType::INTERNAL_ID(), context->getMemoryManager());
        kValueVector = createVector(LogicalType::UINT64(), context->getMemoryManager());
    }

    std::unique_ptr<KCoreOutputWriter> copy() const {
        return std::make_unique<KCoreOutputWriter>(context);
    }

public:
    std::unique_ptr<ValueVector> nodeIDVector;
    std::unique_ptr<ValueVector> kValueVector;
};

class OutputVertexCompute : public VertexCompute {
public:
    OutputVertexCompute(MemoryManager* mm, processor::GDSCallSharedState* sharedState,
        std::unique_ptr<KCoreOutputWriter> writer, CoreValues& coreValues)
        : mm{mm}, sharedState{sharedState}, writer{std::move(writer)}, coreValues{coreValues} {
        localFT = sharedState->claimLocalTable(mm);
    }
    ~OutputVertexCompute() override { sharedState->returnLocalTable(localFT); }

    bool beginOnTable(table_id_t tableID) override {
        coreValues.pinTable(tableID);
        return true;
    }

    void vertexCompute(offset_t startOffset, offset_t endOffset, table_id_t tableID) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            auto nodeID = nodeID_t{i, tableID};
            writer->nodeIDVector->setValue<nodeID_t>(0, nodeID);
            writer->kValueVector->setValue<uint64_t>(0, coreValues.getValue(i));
            localFT->append(writer->vectors);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<OutputVertexCompute>(mm, sharedState, writer->copy(), coreValues);
    }

private:
    MemoryManager* mm;
    processor::GDSCallSharedState* sharedState;
    std::unique_ptr<KCoreOutputWriter> writer;
    CoreValues& coreValues;

    processor::FactorizedTable* localFT;
};

class DegreeLessThanCoreVertexCompute : public VertexCompute {
public:
    DegreeLessThanCoreVertexCompute(Degrees& degrees, CoreValues& coreValues,
        FrontierPair* frontierPair, degree_t coreValue, std::atomic<offset_t>& numActiveNodes)
        : degrees{degrees}, coreValues{coreValues}, frontierPair{frontierPair},
          coreValue{coreValue}, numActiveNodes{numActiveNodes} {}

    bool beginOnTable(table_id_t tableID) override {
        frontierPair->pinNextFrontier(tableID);
        degrees.pinTable(tableID);
        coreValues.pinTable(tableID);
        return true;
    }

    void vertexCompute(offset_t startOffset, offset_t endOffset, table_id_t tableID) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            if (coreValues.isValid(i)) { // Core has been computed
                continue;
            }
            auto degree = degrees.getValue(i);
            if (degree <= coreValue) {
                frontierPair->addNodeToNextDenseFrontier(nodeID_t{i, tableID});
                coreValues.setCoreValue(i, coreValue);
                numActiveNodes.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<DegreeLessThanCoreVertexCompute>(degrees, coreValues, frontierPair,
            coreValue, numActiveNodes);
    }

private:
    Degrees& degrees;
    CoreValues& coreValues;
    FrontierPair* frontierPair;
    degree_t coreValue;
    std::atomic<offset_t>& numActiveNodes;
};

class KCoreDecomposition final : public GDSAlgorithm {
    static constexpr char GROUP_ID_COLUMN_NAME[] = "k_degree";

public:
    KCoreDecomposition() = default;
    KCoreDecomposition(const KCoreDecomposition& other) : GDSAlgorithm{other} {}

    std::vector<LogicalTypeID> getParameterTypeIDs() const override {
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
        auto mm = clientContext->getMemoryManager();
        auto graph = sharedState->graph.get();
        auto numNodesMap = graph->getNumNodesMap(clientContext->getTransaction());
        auto degrees = Degrees(numNodesMap, mm);
        DegreesUtils::computeDegree(context, graph, &degrees, ExtendDirection::BOTH);
        auto coreValues = CoreValues(numNodesMap, mm);
        auto auxiliaryState = std::make_unique<KCoreAuxiliaryState>(degrees, coreValues);
        auto currentFrontier = getPathLengthsFrontier(context, PathLengths::UNVISITED);
        auto nextFrontier = getPathLengthsFrontier(context, PathLengths::UNVISITED);
        auto frontierPair =
            std::make_unique<DoublePathLengthsFrontierPair>(currentFrontier, nextFrontier);
        // Compute Core values
        auto removeVertexEdgeCompute = std::make_unique<RemoveVertexEdgeCompute>(degrees);
        auto computeState =
            GDSComputeState(std::move(frontierPair), std::move(removeVertexEdgeCompute),
                std::move(auxiliaryState), sharedState->getOutputNodeMaskMap());
        auto coreValue = 0u;
        auto numNodes = graph->getNumNodes(clientContext->getTransaction());
        auto numNodesComputed = 0u;
        while (numNodes != numNodesComputed) {
            // Compute current core value
            while (true) {
                std::atomic<offset_t> numActiveNodes;
                numActiveNodes.store(0);
                // Find nodes with degree less than current core.
                auto vc = DegreeLessThanCoreVertexCompute(degrees, coreValues,
                    computeState.frontierPair.get(), coreValue, numActiveNodes);
                GDSUtils::runVertexCompute(context, sharedState->graph.get(), vc);
                numNodesComputed += numActiveNodes.load();
                if (numActiveNodes.load() == 0) {
                    break;
                }
                // Remove found nodes by decreasing their nbrs degree by one.
                computeState.frontierPair->setActiveNodesForNextIter();
                computeState.frontierPair->getNextSparseFrontier().disable();
                GDSUtils::runFrontiersUntilConvergence(context, computeState, graph,
                    ExtendDirection::BOTH,
                    computeState.frontierPair->getCurrentIter() + 1 /* maxIters */);
                // Repeat until all remaining nodes has degree greater than current core.
            }
            coreValue++;
        }
        // Write output
        auto writer = std::make_unique<KCoreOutputWriter>(clientContext);
        auto vertexCompute = OutputVertexCompute(clientContext->getMemoryManager(),
            sharedState.get(), std::move(writer), coreValues);
        GDSUtils::runVertexCompute(context, sharedState->graph.get(), vertexCompute);
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
