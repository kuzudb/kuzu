#include <vector>

#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "common/exception/runtime.h"
#include "common/types/types.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/gds_object_manager.h"
#include "function/gds/gds_utils.h"
#include "function/gds/output_writer.h"
#include "function/gds_function.h"
#include "graph/graph.h"
#include "processor/execution_context.h"
#include "processor/result/factorized_table.h"

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

class SCCState {
public:
    SCCState(const table_id_map_t<offset_t>& numNodesMap, MemoryManager* mm) {
        if (numNodesMap.size() != 1) {
            throw RuntimeException("Kosaraju's SCC only supports operations on one node table.");
        }
        for (const auto& [tableID, numNodes] : numNodesMap) {
            this->numNodes = numNodes;
            this->tableID = tableID;
            componentIDsMap.allocate(tableID, numNodes, mm);
            componentIDs = componentIDsMap.getData(tableID);
            for (auto i = 0u; i < numNodes; ++i) {
                componentIDs[i] = NOT_VISITED;
            }
        }
    }

    void setVisited(const offset_t offset) const { componentIDs[offset] = VISITED; }

    bool visited(const offset_t offset) const { return componentIDs[offset] >= VISITED; }

    void setProcessed(const offset_t offset) const { componentIDs[offset] = PROCESSED; }

    bool processed(const offset_t offset) const { return componentIDs[offset] == PROCESSED; }

    bool componentIDSet(const offset_t offset) const { return componentIDs[offset] < NOT_VISITED; }

    void setComponentID(const offset_t offset, const offset_t value) const {
        componentIDs[offset] = value;
    }

    offset_t getComponentID(const offset_t offset) const { return componentIDs[offset]; }

    offset_t getNumNodes() const { return numNodes; }

    table_id_t getTableID() const { return tableID; }

private:
    offset_t* componentIDs = nullptr;
    ObjectArraysMap<offset_t> componentIDsMap;

    offset_t numNodes;
    table_id_t tableID;
};

class SCCCompute {
public:
    SCCCompute(Graph* graph, SCCState& sccState) : graph{graph}, sccState{sccState} {}

    void compute() {
        auto nbrTables = graph->getForwardNbrTableInfos(sccState.getTableID());
        if (nbrTables.size() != 1) {
            throw RuntimeException("Kosaraju's SCC only supports operations on one edge table.");
        }
        auto nbrInfo = nbrTables[0];
        auto relEntry = nbrInfo.relEntry;
        auto scanState = graph->prepareRelScan(relEntry, "");
        for (auto i = 0u; i < sccState.getNumNodes(); ++i) {
            if (!sccState.visited(i)) {
                forwardDFS(i, *scanState);
            }
        }
        KU_ASSERT(toProcess.size() == 0);
        for (auto it = dfsStack.end() - 1; it >= dfsStack.begin(); --it) {
            auto node = *it;
            if (!sccState.componentIDSet(node)) {
                backwardsDFS(node, node, *scanState);
            }
        }
    }

    void forwardDFS(offset_t node, NbrScanState& scanState) {
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
            auto nextNodeID = nodeID_t{nextNode, sccState.getTableID()};
            for (auto chunk : graph->scanFwd(nextNodeID, scanState)) {
                chunk.forEach([&](auto nbrNodeID, auto) {
                    if (!sccState.visited(nbrNodeID.offset)) {
                        toProcess.push_back(nbrNodeID.offset);
                    }
                });
            }
        }
    }

    void backwardsDFS(offset_t node, const offset_t root, NbrScanState& scanState) {
        toProcess.push_back(node);

        while (!toProcess.empty()) {
            auto nextNode = toProcess.back();
            toProcess.pop_back();
            sccState.setComponentID(nextNode, root);
            auto nextNodeID = nodeID_t{nextNode, sccState.getTableID()};
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
    SCCState& sccState;
    vector<offset_t> toProcess;
    vector<offset_t> dfsStack;
};

class SCCOutputWriter : public GDSOutputWriter {
public:
    explicit SCCOutputWriter(main::ClientContext* context) : GDSOutputWriter{context} {
        nodeIDVector = createVector(LogicalType::INTERNAL_ID(), context->getMemoryManager());
        componentIDVector = createVector(LogicalType::UINT64(), context->getMemoryManager());
    }

    unique_ptr<SCCOutputWriter> copy() const { return make_unique<SCCOutputWriter>(context); }

public:
    unique_ptr<ValueVector> nodeIDVector;
    unique_ptr<ValueVector> componentIDVector;
};

class SCCVertexCompute : public VertexCompute {
public:
    SCCVertexCompute(MemoryManager* mm, GDSCallSharedState* sharedState,
        unique_ptr<SCCOutputWriter> writer, SCCState& sccState)
        : mm{mm}, sharedState{sharedState}, writer{std::move(writer)}, sccState{sccState} {
        localFT = sharedState->claimLocalTable(mm);
    }
    ~SCCVertexCompute() override { sharedState->returnLocalTable(localFT); }

    bool beginOnTable(table_id_t /*tableID*/) override { return true; }

    void vertexCompute(const offset_t startOffset, const offset_t endOffset,
        const table_id_t tableID) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            auto nodeID = nodeID_t{i, tableID};
            writer->nodeIDVector->setValue<nodeID_t>(0, nodeID);
            writer->componentIDVector->setValue<uint64_t>(0, sccState.getComponentID(i));
            localFT->append(writer->vectors);
        }
    }

    unique_ptr<VertexCompute> copy() override {
        return make_unique<SCCVertexCompute>(mm, sharedState, writer->copy(), sccState);
    }

private:
    MemoryManager* mm;
    GDSCallSharedState* sharedState;
    unique_ptr<SCCOutputWriter> writer;
    SCCState& sccState;

    FactorizedTable* localFT;
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
        auto nodeOutput = bindNodeOutput(input, graphEntry.getNodeEntries());
        bindData = make_unique<GDSBindData>(std::move(graphEntry), nodeOutput);
    }

    void exec(ExecutionContext* context) override {
        auto clientContext = context->clientContext;
        auto mm = clientContext->getMemoryManager();
        auto graph = sharedState->graph.get();
        auto numNodesMap = graph->getNumNodesMap(clientContext->getTransaction());

        auto sccState = SCCState(numNodesMap, mm);

        auto edgeCompute = make_unique<SCCCompute>(graph, sccState);
        edgeCompute->compute();

        auto writer = make_unique<SCCOutputWriter>(clientContext);
        auto vertexCompute =
            make_unique<SCCVertexCompute>(mm, sharedState.get(), std::move(writer), sccState);
        GDSUtils::runVertexCompute(context, graph, *vertexCompute);

        sharedState->mergeLocalTables();
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
