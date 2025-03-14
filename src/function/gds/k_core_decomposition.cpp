#include "binder/binder.h"
#include "degrees.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/gds_utils.h"
#include "gds_vertex_compute.h"
#include "processor/execution_context.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

class CoreValues {
public:
    CoreValues(const table_id_map_t<offset_t>& maxOffsetMap, MemoryManager* mm) {
        init(maxOffsetMap, mm);
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
    void init(const table_id_map_t<offset_t>& maxOffsetMap, MemoryManager* mm) {
        for (const auto& [tableID, maxOffset] : maxOffsetMap) {
            coreValuesMap.allocate(tableID, maxOffset, mm);
            pinTable(tableID);
            for (auto i = 0u; i < maxOffset; ++i) {
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

class KCoreResultVertexCompute : public GDSResultVertexCompute {
public:
    KCoreResultVertexCompute(MemoryManager* mm, GDSFuncSharedState* sharedState,
        CoreValues& coreValues)
        : GDSResultVertexCompute{mm, sharedState}, coreValues{coreValues} {
        nodeIDVector = createVector(LogicalType::INTERNAL_ID());
        kValueVector = createVector(LogicalType::UINT64());
    }

    void beginOnTableInternal(table_id_t tableID) override { coreValues.pinTable(tableID); }

    void vertexCompute(offset_t startOffset, offset_t endOffset, table_id_t tableID) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            if (skip(i)) {
                continue;
            }
            auto nodeID = nodeID_t{i, tableID};
            nodeIDVector->setValue<nodeID_t>(0, nodeID);
            kValueVector->setValue<uint64_t>(0, coreValues.getValue(i));
            localFT->append(vectors);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<KCoreResultVertexCompute>(mm, sharedState, coreValues);
    }

private:
    CoreValues& coreValues;
    std::unique_ptr<ValueVector> nodeIDVector;
    std::unique_ptr<ValueVector> kValueVector;
};

class DegreeLessThanCoreVertexCompute : public GDSVertexCompute {
public:
    DegreeLessThanCoreVertexCompute(Degrees& degrees, CoreValues& coreValues,
        FrontierPair* frontierPair, degree_t coreValue, std::atomic<offset_t>& numActiveNodes,
        common::NodeOffsetMaskMap* nodeMask)
        : GDSVertexCompute{nodeMask}, degrees{degrees}, coreValues{coreValues},
          frontierPair{frontierPair}, coreValue{coreValue}, numActiveNodes{numActiveNodes} {}

    void beginOnTableInternal(table_id_t tableID) override {
        frontierPair->pinNextFrontier(tableID);
        degrees.pinTable(tableID);
        coreValues.pinTable(tableID);
    }

    void vertexCompute(offset_t startOffset, offset_t endOffset, table_id_t tableID) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            if (skip(i)) {
                continue;
            }
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
            coreValue, numActiveNodes, nodeMask);
    }

private:
    Degrees& degrees;
    CoreValues& coreValues;
    FrontierPair* frontierPair;
    degree_t coreValue;
    std::atomic<offset_t>& numActiveNodes;
};

static common::offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
    auto clientContext = input.context->clientContext;
    auto mm = clientContext->getMemoryManager();
    auto sharedState = input.sharedState->ptrCast<GDSFuncSharedState>();
    auto graph = sharedState->graph.get();
    auto transaction = clientContext->getTransaction();
    auto degrees = Degrees(graph->getMaxOffsetMap(transaction), mm);
    DegreesUtils::computeDegree(input.context, graph, sharedState->getGraphNodeMaskMap(), &degrees,
        ExtendDirection::BOTH);
    auto coreValues = CoreValues(graph->getMaxOffsetMap(transaction), mm);
    auto auxiliaryState = std::make_unique<KCoreAuxiliaryState>(degrees, coreValues);
    auto currentFrontier =
        PathLengths::getUnvisitedFrontier(input.context, sharedState->graph.get());
    auto nextFrontier = PathLengths::getUnvisitedFrontier(input.context, sharedState->graph.get());
    auto frontierPair =
        std::make_unique<DoublePathLengthsFrontierPair>(currentFrontier, nextFrontier);
    // Compute Core values
    auto removeVertexEdgeCompute = std::make_unique<RemoveVertexEdgeCompute>(degrees);
    auto computeState = GDSComputeState(std::move(frontierPair), std::move(removeVertexEdgeCompute),
        std::move(auxiliaryState), nullptr);
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
                computeState.frontierPair.get(), coreValue, numActiveNodes,
                sharedState->getGraphNodeMaskMap());
            GDSUtils::runVertexCompute(input.context, sharedState->graph.get(), vc);
            numNodesComputed += numActiveNodes.load();
            if (numActiveNodes.load() == 0) {
                break;
            }
            // Remove found nodes by decreasing their nbrs degree by one.
            computeState.frontierPair->initGDS();
            GDSUtils::runFrontiersUntilConvergence(input.context, computeState, graph,
                ExtendDirection::BOTH,
                computeState.frontierPair->getCurrentIter() + 1 /* maxIters */);
            // Repeat until all remaining nodes has degree greater than current core.
        }
        coreValue++;
    }
    // Write output
    auto vertexCompute =
        KCoreResultVertexCompute(clientContext->getMemoryManager(), sharedState, coreValues);
    GDSUtils::runVertexCompute(input.context, graph, vertexCompute);
    sharedState->factorizedTablePool.mergeLocalTables();
    return 0;
}

static constexpr char GROUP_ID_COLUMN_NAME[] = "k_degree";

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto graphName = input->getLiteralVal<std::string>(0);
    auto graphEntry = GDSFunction::bindGraphEntry(*context, graphName);
    auto nodeOutput = GDSFunction::bindNodeOutput(*input, graphEntry.getNodeEntries());
    expression_vector columns;
    columns.push_back(nodeOutput->constCast<NodeExpression>().getInternalID());
    columns.push_back(input->binder->createVariable(GROUP_ID_COLUMN_NAME, LogicalType::INT64()));
    return std::make_unique<GDSBindData>(std::move(columns), std::move(graphEntry), nodeOutput);
}

function_set KCoreDecompositionFunction::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<TableFunction>(KCoreDecompositionFunction::name,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY});
    func->bindFunc = bindFunc;
    func->tableFunc = tableFunc;
    func->initSharedStateFunc = GDSFunction::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = [] { return false; };
    func->getLogicalPlanFunc = GDSFunction::getLogicalPlan;
    func->getPhysicalPlanFunc = GDSFunction::getPhysicalPlan;
    result.push_back(std::move(func));
    return result;
}

} // namespace function
} // namespace kuzu
