#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "common/exception/runtime.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/gds_utils.h"
#include "function/gds_function.h"
#include "gds_vertex_compute.h"
#include "processor/execution_context.h"
#include "processor/operator/gds_call_shared_state.h"

using namespace std;
using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

// Use the three largest offset_t values as special markers to avoid allocating another array.
// PROCESSED implies node has been VISITED and added to the backward DFS stack.
static constexpr offset_t PROCESSED = numeric_limits<offset_t>::max();
static constexpr offset_t VISITED = numeric_limits<offset_t>::max() - 1;
static constexpr offset_t NOT_VISITED = numeric_limits<offset_t>::max() - 2;

struct SccComputationState {
    ObjectArray<offset_t> sccIDs;

    SccComputationState(const offset_t numNodes, MemoryManager* mm)
        : sccIDs{ObjectArray<offset_t>(numNodes, mm)} {
        for (auto i = 0u; i < numNodes; ++i) {
            sccIDs.set(i, NOT_VISITED);
        }
    }

    void setVisited(const offset_t offset) { sccIDs.set(offset, VISITED); }

    bool visited(const offset_t offset) { return sccIDs.get(offset) >= VISITED; }

    void setProcessed(const offset_t offset) { sccIDs.set(offset, PROCESSED); }

    bool processed(const offset_t offset) { return sccIDs.get(offset) == PROCESSED; }

    bool componentIDSet(const offset_t offset) { return sccIDs.get(offset) < NOT_VISITED; }

    void setComponentID(const offset_t offset, const offset_t value) { sccIDs.set(offset, value); }

    offset_t getComponentID(const offset_t offset) { return sccIDs.get(offset); }
};

class SCCCompute {
public:
    SCCCompute(Graph* graph, SccComputationState& sccState) : graph{graph}, sccState{sccState} {}

    void compute(const offset_t tableID, const offset_t numNodes) {
        auto nbrTables = graph->getForwardNbrTableInfos(tableID);
        auto nbrInfo = nbrTables[0];
        auto scanState = graph->prepareRelScan(nbrInfo.relEntry, nbrInfo.nodeEntry, "");
        vector<offset_t> toProcess;
        vector<offset_t> dfsStack;
        for (auto i = 0u; i < numNodes; ++i) {
            if (!sccState.visited(i)) {
                forwardDFS(i, tableID, *scanState, toProcess, dfsStack);
            }
        }
        KU_ASSERT(toProcess.size() == 0);
        for (auto it = dfsStack.end() - 1; it >= dfsStack.begin(); --it) {
            auto node = *it;
            if (!sccState.componentIDSet(node)) {
                backwardsDFS(node, node, tableID, *scanState, toProcess);
            }
        }
    }

    void forwardDFS(const offset_t node, const offset_t tableID, NbrScanState& scanState,
        vector<offset_t>& toProcess, vector<offset_t>& dfsStack) {
        toProcess.push_back(node);
        while (!toProcess.empty()) {
            auto nextNode = toProcess.back();
            if (sccState.visited(nextNode)) {
                toProcess.pop_back();
                if (!sccState.processed(nextNode)) {
                    dfsStack.push_back(nextNode);
                    sccState.setProcessed(nextNode);
                }
                continue;
            }
            sccState.setVisited(nextNode);
            auto nextNodeID = nodeID_t{nextNode, tableID};
            for (auto chunk : graph->scanFwd(nextNodeID, scanState)) {
                chunk.forEach([&](auto nbrNodeID, auto) {
                    if (!sccState.visited(nbrNodeID.offset)) {
                        toProcess.push_back(nbrNodeID.offset);
                    }
                });
            }
        }
    }

    void backwardsDFS(offset_t node, const offset_t root, const offset_t tableID,
        NbrScanState& scanState, vector<offset_t>& toProcess) {
        toProcess.push_back(node);
        while (!toProcess.empty()) {
            auto nextNode = toProcess.back();
            toProcess.pop_back();
            sccState.setComponentID(nextNode, root);
            auto nextNodeID = nodeID_t{nextNode, tableID};
            for (auto chunk : graph->scanBwd(nextNodeID, scanState)) {
                chunk.forEach([&](auto nbrNodeID, auto) {
                    if (!sccState.componentIDSet(nbrNodeID.offset)) {
                        toProcess.push_back(nbrNodeID.offset);
                    }
                });
            }
        }
    }

private:
    Graph* graph;
    SccComputationState& sccState;
};

class SCCVertexCompute : public GDSResultVertexCompute {
public:
    SCCVertexCompute(MemoryManager* mm, GDSCallSharedState* sharedState,
        SccComputationState& sccState)
        : GDSResultVertexCompute{mm, sharedState}, sccState{sccState} {
        nodeIDVector = createVector(LogicalType::INTERNAL_ID());
        componentIDVector = createVector(LogicalType::UINT64());
    }

    void beginOnTableInternal(table_id_t /*tableID*/) override {}

    void vertexCompute(const offset_t startOffset, const offset_t endOffset,
        const table_id_t tableID) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            auto nodeID = nodeID_t{i, tableID};
            nodeIDVector->setValue<nodeID_t>(0, nodeID);
            componentIDVector->setValue<uint64_t>(0, sccState.getComponentID(i));
            localFT->append(vectors);
        }
    }

    unique_ptr<VertexCompute> copy() override {
        return std::make_unique<SCCVertexCompute>(mm, sharedState, sccState);
    }

private:
    SccComputationState& sccState;
    unique_ptr<ValueVector> nodeIDVector;
    unique_ptr<ValueVector> componentIDVector;
};

class SCCKosaraju final : public GDSAlgorithm {
    static constexpr char GROUP_ID_COLUMN_NAME[] = "group_id";

public:
    SCCKosaraju() = default;
    SCCKosaraju(const SCCKosaraju& other) : GDSAlgorithm{other} {}

    unique_ptr<GDSAlgorithm> copy() const override { return make_unique<SCCKosaraju>(*this); }

    vector<LogicalTypeID> getParameterTypeIDs() const override {
        return vector<LogicalTypeID>{LogicalTypeID::ANY};
    }

    expression_vector getResultColumns(const GDSBindInput& bindInput) const override {
        expression_vector columns;
        auto& outputNode = bindData->getNodeOutput()->constCast<NodeExpression>();
        columns.push_back(outputNode.getInternalID());
        columns.push_back(
            bindInput.binder->createVariable(GROUP_ID_COLUMN_NAME, LogicalType::INT64()));
        return columns;
    }

    void bind(const GDSBindInput& input, main::ClientContext& context) override {
        auto graphName = ExpressionUtil::getLiteralValue<string>(*input.getParam(0));
        auto graphEntry = bindGraphEntry(context, graphName);
        if (graphEntry.nodeInfos.size() != 1) {
            throw RuntimeException("Kosaraju's SCC only supports operations on one node table.");
        }
        if (graphEntry.relInfos.size() != 1) {
            throw RuntimeException("Kosaraju's SCC only supports operations on one edge table.");
        }
        auto nodeOutput = bindNodeOutput(input, graphEntry.getNodeEntries());
        bindData = make_unique<GDSBindData>(std::move(graphEntry), nodeOutput);
    }

    void exec(ExecutionContext* context) override {
        auto clientContext = context->clientContext;
        auto mm = clientContext->getMemoryManager();
        auto graph = sharedState->graph.get();
        KU_ASSERT(graph->getNodeTableIDs().size() == 1);
        auto tableID = graph->getNodeTableIDs()[0];
        auto maxOffset = graph->getMaxOffset(clientContext->getTransaction(), tableID);

        auto sccState = SccComputationState(maxOffset, mm);
        auto edgeCompute = make_unique<SCCCompute>(graph, sccState);
        edgeCompute->compute(tableID, maxOffset);

        auto vertexCompute = make_unique<SCCVertexCompute>(mm, sharedState.get(), sccState);
        GDSUtils::runVertexCompute(context, graph, *vertexCompute);

        sharedState->factorizedTablePool.mergeLocalTables();
    }
};

function_set SCCKosarajuFunction::getFunctionSet() {
    function_set result;
    auto algo = make_unique<SCCKosaraju>();
    auto function = make_unique<GDSFunction>(name, algo->getParameterTypeIDs(), std::move(algo));
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
