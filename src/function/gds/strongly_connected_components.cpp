#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "common/exception/runtime.h"
#include "common/types/types.h"
#include "function/gds/auxiliary_state/gds_auxilary_state.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/gds_object_manager.h"
#include "function/gds/gds_utils.h"
#include "function/gds/output_writer.h"
#include "function/gds_function.h"
#include "gds_vertex_compute.h"
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

static constexpr offset_t SCC_UNSET = std::numeric_limits<offset_t>::max();

struct SCCComputationState : GDSAuxiliaryState {
    std::atomic<bool> allSccIdsSet;
    AtomicObjectArray<offset_t> sccIDs;
    AtomicObjectArray<offset_t> fwdColors;
    AtomicObjectArray<offset_t> bwdColors;

    SCCComputationState(const offset_t numNodes, MemoryManager* mm)
        : sccIDs{AtomicObjectArray<offset_t>(numNodes, mm)},
          fwdColors{AtomicObjectArray<offset_t>(numNodes, mm)},
          bwdColors{AtomicObjectArray<offset_t>(numNodes, mm)} {
        allSccIdsSet.store(false, std::memory_order_relaxed);
    }

    bool update(offset_t boundOffset, offset_t nbrOffset, bool isFwd) {
        auto& colors = isFwd ? fwdColors : bwdColors;
        return colors.compare_exchange_max(boundOffset, nbrOffset);
    }

    bool isSccIdSet(offset_t nodeId) { return sccIDs.getRelaxed(nodeId) != SCC_UNSET; }

    // Start by assuming all scc ids are set. `memory_order_seq_cst` is used to ensure that
    // all threads see the initial value of `allSccIdsSet`.
    void reset() { allSccIdsSet.store(true, std::memory_order_seq_cst); }

    void beginFrontierCompute(common::table_id_t, common::table_id_t) override {}
};

class SetInitialSccIds : public VertexCompute {
public:
    explicit SetInitialSccIds(SCCComputationState& computationState)
        : computationState{computationState} {}

    bool beginOnTable(table_id_t) override { return true; }

    void vertexCompute(offset_t startOffset, offset_t endOffset, table_id_t) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            computationState.sccIDs.setRelaxed(i, SCC_UNSET);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<SetInitialSccIds>(computationState);
    }

private:
    SCCComputationState& computationState;
};

class FindNewSccIds : public VertexCompute {
public:
    explicit FindNewSccIds(SCCComputationState& computationState)
        : computationState{computationState} {}

    bool beginOnTable(table_id_t) override { return true; }

    void vertexCompute(offset_t startOffset, offset_t endOffset, table_id_t) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            if (!computationState.isSccIdSet(i)) {
                auto fwdColor = computationState.fwdColors.getRelaxed(i);
                auto bwdColor = computationState.bwdColors.getRelaxed(i);
                if (fwdColor == bwdColor) {
                    computationState.sccIDs.setRelaxed(i, fwdColor);
                } else {
                    // If any of the threads find an unset scc id, they will set the flag to false.
                    // `allSccIdsSet` is never read and written concurrently, so it does
                    // not need to be atomic.
                    computationState.allSccIdsSet.store(false, std::memory_order_relaxed);
                }
            }
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<FindNewSccIds>(computationState);
    }

private:
    SCCComputationState& computationState;
};

class SetInitialColors : public VertexCompute {
public:
    explicit SetInitialColors(SCCComputationState& computationState)
        : computationState{computationState} {}

    bool beginOnTable(table_id_t) override { return true; }

    void vertexCompute(offset_t startOffset, offset_t endOffset, table_id_t) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            computationState.fwdColors.setRelaxed(i, i);
            computationState.bwdColors.setRelaxed(i, i);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<SetInitialColors>(computationState);
    }

private:
    SCCComputationState& computationState;
};

class InitializeFrontiers : public VertexCompute {
public:
    InitializeFrontiers(FrontierPair& frontierPair, SCCComputationState& computationState)
        : frontierPair{frontierPair}, computationState{computationState} {}

    bool beginOnTable(table_id_t tableId) override {
        frontierPair.pinCurrFrontier(tableId);
        frontierPair.pinNextFrontier(tableId);
        return true;
    }

    void vertexCompute(offset_t startOffset, offset_t endOffset, table_id_t) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            // If the SCC ID has already been computed, the node should not be activated.
            auto initialState = computationState.isSccIdSet(i) ? PathLengths::INITIAL_VISITED :
                                                                 PathLengths::UNVISITED;
            frontierPair.getCurDenseFrontier().setCurFrontierValue(i, initialState);
            frontierPair.getNextDenseFrontier().setCurFrontierValue(i,
                PathLengths::INITIAL_VISITED);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<InitializeFrontiers>(frontierPair, computationState);
    }

private:
    FrontierPair& frontierPair;
    SCCComputationState& computationState;
};

struct ComputeColors : public EdgeCompute {
    SCCComputationState& computationState;

    explicit ComputeColors(SCCComputationState& computationState)
        : computationState{computationState} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk,
        bool isFwd) override {
        std::vector<nodeID_t> result;
        if (computationState.isSccIdSet(boundNodeID.offset)) {
            return result;
        }
        chunk.forEach([&](auto nbrNodeID, auto) {
            if (computationState.isSccIdSet(nbrNodeID.offset)) {
            } else if (computationState.update(boundNodeID.offset, nbrNodeID.offset, isFwd)) {
                result.push_back(nbrNodeID);
            }
        });
        return result;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<ComputeColors>(computationState);
    }
};

class SccOutputWriter : public GDSOutputWriter {
public:
    explicit SccOutputWriter(main::ClientContext* context) : GDSOutputWriter{context} {
        nodeIDVector = createVector(LogicalType::INTERNAL_ID(), context->getMemoryManager());
        sccIDVector = createVector(LogicalType::UINT64(), context->getMemoryManager());
    }

    std::unique_ptr<SccOutputWriter> copy() const {
        return std::make_unique<SccOutputWriter>(context);
    }

public:
    std::unique_ptr<ValueVector> nodeIDVector;
    std::unique_ptr<ValueVector> sccIDVector;
};

class WriteSccIdsToOutput : public GDSResultVertexCompute {
public:
    WriteSccIdsToOutput(storage::MemoryManager* mm, processor::GDSCallSharedState* sharedState,
        std::unique_ptr<SccOutputWriter> writer, SCCComputationState& computationState)
        : GDSResultVertexCompute{mm, sharedState}, writer{std::move(writer)},
          computationState{computationState} {}

    void beginOnTableInternal(table_id_t) override {}

    void vertexCompute(offset_t startOffset, offset_t endOffset, table_id_t tableID) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            if (skip(i)) {
                continue;
            }
            auto nodeID = nodeID_t{i, tableID};
            writer->nodeIDVector->setValue<nodeID_t>(0, nodeID);
            writer->sccIDVector->setValue<uint64_t>(0, computationState.sccIDs.getRelaxed(i));
            localFT->append(writer->vectors);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<WriteSccIdsToOutput>(mm, sharedState, writer->copy(),
            computationState);
    }

private:
    std::unique_ptr<SccOutputWriter> writer;
    SCCComputationState& computationState;
};

class SCC final : public GDSAlgorithm {
    static constexpr char GROUP_ID_COLUMN_NAME[] = "group_id";
    static constexpr uint8_t MAX_ITERATION = 100;

public:
    SCC() = default;
    SCC(const SCC& other) : GDSAlgorithm{other} {}

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
        if (graphEntry.nodeInfos.size() != 1) {
            throw RuntimeException("Parallel SCC only supports operations on one node table.");
        }
        if (graphEntry.relInfos.size() != 1) {
            throw RuntimeException("Parallel SCC only supports operations on one edge table.");
        }
        auto nodeOutput = bindNodeOutput(input, graphEntry.getNodeEntries());
        bindData = std::make_unique<GDSBindData>(std::move(graphEntry), nodeOutput);
    }

    void exec(processor::ExecutionContext* context) override {
        auto clientContext = context->clientContext;
        auto mm = clientContext->getMemoryManager();
        auto graph = sharedState->graph.get();
        auto getMaxOffsetMap = graph->getMaxOffsetMap(clientContext->getTransaction());
        auto it = getMaxOffsetMap.begin();
        auto numNodes = it->second;

        auto computationState = SCCComputationState(numNodes, mm);
        // Initialize SCC IDs to unset.
        auto setInitialSccIds = std::make_unique<SetInitialSccIds>(computationState);
        GDSUtils::runVertexCompute(context, graph, *setInitialSccIds);

        // The frontiers will be initialized inside the loop.
        auto currentFrontier = std::make_shared<PathLengths>(getMaxOffsetMap, mm);
        auto nextFrontier = std::make_shared<PathLengths>(getMaxOffsetMap, mm);
        auto frontierPair =
            std::make_unique<DoublePathLengthsFrontierPair>(currentFrontier, nextFrontier);

        auto setNewSccIds = std::make_unique<FindNewSccIds>(computationState);
        auto setInitialColors = std::make_unique<SetInitialColors>(computationState);
        auto computeColors = std::make_unique<ComputeColors>(computationState);
        auto auxiliaryState = std::make_unique<EmptyGDSAuxiliaryState>();
        auto computeState = GDSComputeState(std::move(frontierPair), std::move(computeColors),
            std::move(auxiliaryState), sharedState->getOutputNodeMaskMap());
        auto initializeFrontiers =
            std::make_unique<InitializeFrontiers>(*computeState.frontierPair, computationState);

        for (auto i = 0; i < MAX_ITERATION; i++) {
            GDSUtils::runVertexCompute(context, graph, *setInitialColors);

            // Fwd colors.
            computeState.frontierPair->initState();
            computeState.frontierPair->initGDS();
            GDSUtils::runVertexCompute(context, graph, *initializeFrontiers);
            GDSUtils::runFrontiersUntilConvergence(context, computeState, graph,
                ExtendDirection::FWD, MAX_ITERATION);

            // Bwd colors.
            computeState.frontierPair->initState();
            computeState.frontierPair->initGDS();
            GDSUtils::runVertexCompute(context, graph, *initializeFrontiers);
            GDSUtils::runFrontiersUntilConvergence(context, computeState, graph,
                ExtendDirection::BWD, MAX_ITERATION);

            // Find new SCC IDs and exit if all IDs have been found.
            computationState.reset();
            GDSUtils::runVertexCompute(context, graph, *setNewSccIds);
            if (computationState.allSccIdsSet.load(std::memory_order_relaxed)) {
                break;
            }
        }

        auto sccOutputWriter = std::make_unique<SccOutputWriter>(clientContext);
        auto writeSccIdsToOutput = std::make_unique<WriteSccIdsToOutput>(mm, sharedState.get(),
            std::move(sccOutputWriter), computationState);
        GDSUtils::runVertexCompute(context, graph, *writeSccIdsToOutput);

        sharedState->mergeLocalTables();
    }

    std::unique_ptr<GDSAlgorithm> copy() const override { return std::make_unique<SCC>(*this); }
};

function_set SCCFunction::getFunctionSet() {
    function_set result;
    auto algo = std::make_unique<SCC>();
    result.push_back(
        std::make_unique<GDSFunction>(name, algo->getParameterTypeIDs(), std::move(algo)));
    return result;
}

} // namespace function
} // namespace kuzu
