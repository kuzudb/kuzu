#include <thread>
#include <tuple>
#include <vector>

#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "common/types/types.h"
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
#include <format>

using namespace std;
using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

static constexpr offset_t NOT_ASSIGNED = std::numeric_limits<offset_t>::max();

/// State for the SCC algorithm
class SCCState {
public:
    SCCState(const table_id_map_t<offset_t>& numNodesMap, MemoryManager* mm) {
        for (const auto& [tableID, numNodes] : numNodesMap) {
            componentIDsMap.allocate(tableID, numNodes, mm);
            visitsMap.allocate(tableID, numNodes, mm);
            pinTable(tableID);
            for (auto i = 0u; i < numNodes; ++i) {
                visits[i].store(false, std::memory_order_relaxed);
                componentIDs[i].store(NOT_ASSIGNED, std::memory_order_relaxed);
            }
        }
    }

    void pinTable(table_id_t tableID) {
        componentIDs = componentIDsMap.getData(tableID);
        visits = visitsMap.getData(tableID);
    }

    bool setVisit(common::offset_t offset, bool value) {
        visits[offset].store(value, std::memory_order_relaxed);
        return true;
    }

    bool getVisit(offset_t offset) { return visits[offset].load(std::memory_order_relaxed); }

    bool setComponentID(common::offset_t offset, offset_t value) {
        componentIDs[offset].store(value, std::memory_order_relaxed);
        return true;
    }

    offset_t getComponentID(offset_t offset) {
        return componentIDs[offset].load(std::memory_order_relaxed);
    }

private:
    // Pointers to current tableID's visits and componentIDs
    std::atomic<bool>* visits = nullptr;
    std::atomic<offset_t>* componentIDs = nullptr;
    // State for visits and componentIDs
    ObjectArraysMap<std::atomic<bool>> visitsMap;
    ObjectArraysMap<std::atomic<offset_t>> componentIDsMap;
};

class SCCCompute {
public:
    SCCCompute(graph::Graph* graph, const table_id_map_t<offset_t>& numNodesMap, SCCState& sccState)
        : graph{graph}, sccState{sccState}, numNodesMap{numNodesMap} {};

    void forwardDFS(nodeID_t node, catalog::TableCatalogEntry* relEntry) {
        if (sccState.getVisit(node.offset)) {
            // Already visited
            return;
        };
        sccState.setVisit(node.offset, true);
        auto scanState = graph->prepareRelScan(relEntry, "");
        for (auto chunk : graph->scanFwd(node, *scanState)) {
            chunk.forEach([&](auto nbrNodeID, auto) { forwardDFS(nbrNodeID, relEntry); });
        }
        queue.push_back(node);
    }

    // Backwards DFS
    void backwardsDFS(nodeID_t node, offset_t root, catalog::TableCatalogEntry* relEntry) {
        if (sccState.getComponentID(node.offset) != NOT_ASSIGNED) {
            // Already assigned
            return;
        };
        sccState.setComponentID(node.offset, root);
        auto scanState = graph->prepareRelScan(relEntry, "");
        for (auto chunk : graph->scanBwd(node, *scanState)) {
            chunk.forEach([&](auto nbrNodeID, auto) { backwardsDFS(nbrNodeID, root, relEntry); });
        }
    }

    void compute() {
        for (const auto& [tableID, numNodes] : numNodesMap) {
            sccState.pinTable(tableID);
            for (auto& [fromEntry, toEntry, relEntry] : graph->getRelFromToEntryInfos()) {
                for (auto i = 0u; i < numNodes; ++i) {
                    auto nodeID = nodeID_t{i, tableID};
                    forwardDFS(nodeID, relEntry);
                }
                for (auto it = queue.rbegin(); it != queue.rend(); ++it) {
                    auto node = *it;
                    backwardsDFS(node, node.offset, relEntry);
                }
            }
        }
    }

private:
    std::vector<nodeID_t> queue;
    graph::Graph* graph;
    SCCState& sccState;
    const table_id_map_t<offset_t>& numNodesMap;
};

class SCCOutputWriter : public GDSOutputWriter {
public:
    explicit SCCOutputWriter(main::ClientContext* context) : GDSOutputWriter{context} {
        nodeIDVector = createVector(LogicalType::INTERNAL_ID(), context->getMemoryManager());
        componentIDVector = createVector(LogicalType::UINT64(), context->getMemoryManager());
    }

    std::unique_ptr<SCCOutputWriter> copy() const {
        return std::make_unique<SCCOutputWriter>(context);
    }

public:
    std::unique_ptr<ValueVector> nodeIDVector;
    std::unique_ptr<ValueVector> componentIDVector;
};

class SCCVertexCompute : public VertexCompute {
public:
    SCCVertexCompute(storage::MemoryManager* mm, processor::GDSCallSharedState* sharedState,
        std::unique_ptr<SCCOutputWriter> writer, SCCState& sccState)
        : mm{mm}, sharedState{sharedState}, writer{std::move(writer)}, sccState{sccState} {
        localFT = sharedState->claimLocalTable(mm);
    }
    ~SCCVertexCompute() override { sharedState->returnLocalTable(localFT); }

    bool beginOnTable(common::table_id_t tableID) override {
        sccState.pinTable(tableID);
        return true;
    }

    void vertexCompute(common::offset_t startOffset, common::offset_t endOffset,
        common::table_id_t tableID) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            auto nodeID = nodeID_t{i, tableID};
            writer->nodeIDVector->setValue<nodeID_t>(0, nodeID);
            writer->componentIDVector->setValue<uint64_t>(0, sccState.getComponentID(i));
            localFT->append(writer->vectors);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<SCCVertexCompute>(mm, sharedState, writer->copy(), sccState);
    }

private:
    storage::MemoryManager* mm;
    processor::GDSCallSharedState* sharedState;
    std::unique_ptr<SCCOutputWriter> writer;
    SCCState& sccState;

    processor::FactorizedTable* localFT;
};

class StronglyConnectedComponent final : public GDSAlgorithm {
    static constexpr char GROUP_ID_COLUMN_NAME[] = "group_id";
    static constexpr uint8_t MAX_ITERATION = 100;

public:
    StronglyConnectedComponent() = default;
    StronglyConnectedComponent(const StronglyConnectedComponent& other) : GDSAlgorithm{other} {}

    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
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
        auto nodeOutput = bindNodeOutput(input, graphEntry.nodeEntries);
        bindData = std::make_unique<GDSBindData>(std::move(graphEntry), nodeOutput);
    }

    void exec(processor::ExecutionContext* context) override {
        auto clientContext = context->clientContext;
        auto graph = sharedState->graph.get();
        auto numNodesMap = graph->getNumNodesMap(clientContext->getTransaction());
        auto sccState = SCCState(numNodesMap, clientContext->getMemoryManager());
        auto writer = std::make_unique<SCCOutputWriter>(clientContext);
        auto vertexCompute = std::make_unique<SCCVertexCompute>(clientContext->getMemoryManager(),
            sharedState.get(), std::move(writer), sccState);
        auto edgeCompute = std::make_unique<SCCCompute>(graph, numNodesMap, sccState);
        edgeCompute->compute();
        GDSUtils::runVertexCompute(context, graph, *vertexCompute);
        sharedState->mergeLocalTables();
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<StronglyConnectedComponent>(*this);
    }
};

function_set StronglyConnectedComponentsFunction::getFunctionSet() {
    function_set result;
    auto algo = std::make_unique<StronglyConnectedComponent>();
    auto function =
        std::make_unique<GDSFunction>(name, algo->getParameterTypeIDs(), std::move(algo));
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
